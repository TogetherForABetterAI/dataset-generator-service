import logging
import queue
from typing import Optional
from src.middleware.middleware import RabbitMQMiddleware
from src.db.client import DatabaseClient

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """
    Handles graceful shutdown of the service.
    Follows the Go ShutdownHandler pattern exactly.

    Orchestrates shutdown based on two possible triggers:
    1. Server error/completion (server_done)
    2. OS signal (SIGTERM/SIGINT)
    """

    def __init__(
        self,
        listener,
        middleware: RabbitMQMiddleware,
        db_client: DatabaseClient,
    ):
        """
        Initialize the shutdown handler.

        Args:
            listener: The listener instance (with get_consumer_tag and interrupt_workers methods)
            middleware: The middleware instance to close
            db_client: The database client to close
        """
        self.listener = listener
        self.middleware = middleware
        self.db_client = db_client

    def handle_shutdown(self, shutdown_queue: queue.Queue) -> Optional[Exception]:
        """
        Orchestrates graceful shutdown based on shutdown sources.

        Waits (blocks) on a single queue until a shutdown trigger arrives.
        The queue receives the error (Exception or None) from either:
        1. Server error/completion
        2. OS signal handler

        Args:
            shutdown_queue: Single queue that receives shutdown events

        Returns:
            Exception if shutdown encountered an error, None otherwise
        """
        # Block here until a shutdown event arrives
        err: Optional[Exception] = shutdown_queue.get(block=True)

        logger.info("Shutdown triggered, initiating graceful shutdown")
        self.shutdown()

        if err is not None:
            logger.error(f"Service stopped with an error: {err}", exc_info=True)
            return err
        logger.info("Service stopped cleanly")
        return None

    def shutdown(self):
        """
        Initiates the shutdown of all server components.
        Always interrupts ongoing processing.

        Shutdown order:
        1. Stop consuming new messages from RabbitMQ
        2. Interrupt all active clients/workers
        3. Close middleware connection
        4. Close database connection pool
        """
        logger.info("Shutting down server components...")

        # Step 1: Stop consuming new messages from RabbitMQ
        try:
            consumer_tag = self.listener.get_consumer_tag()
            logger.info(f"Stopping consumer with tag: {consumer_tag}")
            self.middleware.stop_consuming(consumer_tag)
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}", exc_info=True)

        # Step 2: Interrupt all active workers
        # This triggers graceful shutdown: workers finish current jobs, then stop
        try:
            logger.info("Interrupting workers...")
            self.listener.interrupt_workers()
        except Exception as e:
            logger.error(f"Error interrupting workers: {e}", exc_info=True)

        # Step 3: Close middleware connection
        try:
            logger.info("Closing middleware...")
            self.middleware.close()
        except Exception as e:
            logger.error(f"Error closing middleware: {e}", exc_info=True)

        # Step 4: Close database connection pool
        try:
            logger.info("Closing database client...")
            self.db_client.close()
        except Exception as e:
            logger.error(f"Error closing database: {e}", exc_info=True)

        logger.info("Server shutdown complete")
