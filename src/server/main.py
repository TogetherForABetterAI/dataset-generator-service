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

        # Initialize database client
        self.db_client = DatabaseClient(config.database_config)

        # Initialize listener
        self.listener = Listener(config)

        # Initialize shutdown handler
        self.shutdown_handler = ShutdownHandler(
            listener=self.listener,
            middleware=self.middleware,
            db_client=self.db_client,
        )

        # Shutdown coordination channels (Go pattern using Queues)
        self.server_done = queue.Queue(maxsize=1)  # Server completion/error channel
        self.os_signals = queue.Queue(maxsize=1)  # OS signal channel

        logger.info(
            f"Server initialized successfully (Pod: {config.pod_name}, "
            f"Workers: {config.worker_pool_size})"
        )

    def run(self):
        """
        Start the server and begin processing messages.
        This method blocks until the server is stopped.

        Follows Go pattern:
        1. Setup signal handlers that write to os_signals queue
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

            # Setup signal handlers that write to os_signals queue
            self._setup_signal_handlers()

            # Start the server components in a separate thread
            # This thread will put to server_done when it completes or errors
            server_thread = threading.Thread(target=self._run_server, daemon=False)
            server_thread.start()

            # Block here waiting for shutdown trigger
            # This mimics Go's select on serverDone/osSignals channels
            err = self.shutdown_handler.handle_shutdown(
                self.server_done, self.os_signals
            )

            # Wait for server thread to finish
            server_thread.join(timeout=30)

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
        Signals are written to the os_signals queue.
        """

        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logger.info(f"Received signal {sig_name}")
            try:
                self.os_signals.put(sig_name, block=False)
            except queue.Full:
                logger.warning("os_signals queue is full, signal already pending")

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info("Signal handlers registered for SIGTERM and SIGINT")

    def _run_server(self):
        """
        Run the server components in background thread.
        Puts completion/error to server_done queue.
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
            # Put error or None to server_done
            try:
                self.server_done.put(err, block=False)
            except queue.Full:
                logger.warning("server_done queue already has a value")

    def stop(self):
        """
        Stop the server gracefully.
        This is called by the shutdown handler.
        """
        logger.info("Stopping Dataset Generation Service...")
        self.shutdown_handler.shutdown_clients()
        logger.info("Dataset Generation Service stopped")
