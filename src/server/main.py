import logging
import threading
import signal
import queue
from typing import Optional
from src.config.config import GlobalConfig
from src.server.listener import Listener
from src.server.shutdown_handler import ShutdownHandler
from src.middleware.middleware import RabbitMQMiddleware
from src.db.client import DatabaseClient

logger = logging.getLogger(__name__)


class Server:
    """
    Main server class that manages the Dataset Generation Service lifecycle.
    This service consumes messages from RabbitMQ, generates dataset batches,
    and persists them to PostgreSQL.

    Follows the Go server pattern with graceful shutdown handling via channels (Queues).
    """

    def __init__(self, config: GlobalConfig):
        """
        Initialize the server with all necessary components.

        Args:
            config: Global configuration
        """
        self.config = config

        # Initialize middleware
        self.middleware = RabbitMQMiddleware(config.middleware_config)

        # Initialize listener
        self.listener = Listener(config)

        # Initialize shutdown handler
        self.shutdown_handler = ShutdownHandler(
            listener=self.listener,
        )

        # Shutdown coordination queue (single queue for both sources)
        self.shutdown_queue = queue.Queue(maxsize=2)  # Can hold both events if needed

        logger.info(
            f"Server initialized successfully (Pod: {config.pod_name}, "
            f"Workers: {config.worker_pool_size})"
        )

    def run(self):
        """
        Start the server and begin processing messages.
        This method blocks until the server is stopped.

        Follows Go pattern:
        1. Setup signal handlers that write to shutdown_queue
        2. Start server components in background thread
        3. Call shutdown_handler.handle_shutdown() which blocks until shutdown trigger
        """
        try:
            logger.info("=" * 60)
            logger.info(f"Starting Dataset Generation Service")
            logger.info(f"Pod Name: {self.config.pod_name}")
            logger.info(f"Worker Pool Size: {self.config.worker_pool_size}")
            logger.info(
                f"RabbitMQ: {self.config.middleware_config.host}:{self.config.middleware_config.port}"
            )
            logger.info(
                f"PostgreSQL: {self.config.database_config.host}:{self.config.database_config.port}/{self.config.database_config.dbname}"
            )
            logger.info("=" * 60)

            # Setup signal handlers that write to shutdown_queue
            self._setup_signal_handlers()

            # Start the server components in a separate thread
            # This thread will put to shutdown_queue when it completes or errors
            server_thread = threading.Thread(target=self._run_server, daemon=False)
            server_thread.start()

            # Block here waiting for shutdown trigger
            err = self.shutdown_handler.handle_shutdown(self.shutdown_queue)

            # Wait for server thread to finish
            server_thread.join()

            if err is not None:
                logger.error(f"Server stopped with error: {err}")
                raise err

            logger.info("Server stopped cleanly")

        except Exception as e:
            logger.error(f"Fatal error in server: {e}", exc_info=True)
            raise
        finally:
            logger.info("Server run() completed")

    def _setup_signal_handlers(self):
        """
        Setup signal handlers for SIGTERM and SIGINT.
        When a signal is received, None is put in shutdown_queue (clean shutdown).
        """

        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logger.info(f"Received signal {sig_name}")
            try:
                # Signal means clean shutdown (no error)
                self.shutdown_queue.put(None, block=False)
            except queue.Full:
                logger.warning("shutdown_queue is full, signal already pending")

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info("Signal handlers registered for SIGTERM and SIGINT")

    def _run_server(self):
        """
        Run the server components in background thread.
        Puts the error (or None if clean) to shutdown_queue.
        """
        err: Optional[Exception] = None
        try:
            logger.info("Starting listener...")
            # This blocks until listener stops
            self.listener.start()
            logger.info("Listener stopped normally")
        except Exception as e:
            logger.error(f"Error in server components: {e}", exc_info=True)
            err = e
        finally:
            # Put error to shutdown_queue
            try:
                self.shutdown_queue.put(err, block=False)
            except queue.Full:
                logger.warning(
                    "shutdown_queue is full, server completion event not sent"
                )
