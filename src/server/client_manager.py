import logging
from typing import Dict, Any
from src.config.config import GlobalConfig
from src.middleware.middleware import RabbitMQMiddleware
from src.db.client import DatabaseClient
from src.repository.batch_repository import BatchRepository
from src.server.batch_handler import BatchHandler

logger = logging.getLogger(__name__)


class ClientManager:
    """
    Manages the processing of a single client session.
    Handles batch generation, database persistence, and notification to dispatcher.

    This class is similar to the ClientManager in Go - it encapsulates all the logic
    for handling a single client/session request.
    """

    def __init__(
        self,
        config: GlobalConfig,
        channel: Any,  # RabbitMQ channel
        db_client: DatabaseClient,
        batch_handler: BatchHandler,
        session_id: str,
    ):
        """
        Initialize a ClientManager for a specific session.

        Args:
            config: Global configuration
            channel: RabbitMQ channel (exclusive to this worker)
            db_client: Database client
            batch_handler: Batch handler for generating batches
            session_id: The session ID being processed
        """
        self.config = config
        self.channel = channel
        self.db_client = db_client
        self.repository = BatchRepository(db_client)
        self.batch_handler = batch_handler
        self.session_id = session_id
        self.stopped = False

    def handle_client(self, notification: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a client notification: generate batches, save to DB, and notify dispatcher.

        Args:
            notification: Dictionary containing session_id, dataset_name, batch_size, etc.

        Returns:
            Dictionary with processing results including total_batches

        Raises:
            Exception: If processing fails
        """
        if self.stopped:
            logger.warning(f"ClientManager for session {self.session_id} was stopped")
            return {}

        dataset_name = notification.get("dataset_name", "mnist")
        batch_size = notification.get("batch_size", 64)

        logger.info(
            f"ClientManager: Processing session_id={self.session_id}, "
            f"dataset={dataset_name}, batch_size={batch_size}"
        )

        try:
            # Generate all batches
            batches, num_batches = self.batch_handler.generate_batches(
                dataset_name, batch_size
            )

            if self.stopped:
                logger.warning(
                    f"ClientManager for session {self.session_id} stopped during batch generation"
                )
                return {}

            # Save batches to database in a single transaction
            logger.info(
                f"ClientManager: Saving {num_batches} batches for session {self.session_id}"
            )
            self._save_batches(batches)

            if self.stopped:
                logger.warning(
                    f"ClientManager for session {self.session_id} stopped during save"
                )
                return {}

            logger.info(
                f"ClientManager: Successfully completed processing for session {self.session_id}"
            )

            # Return results
            return {
                "session_id": self.session_id,
                "dataset_name": dataset_name,
                "total_batches": num_batches,
                "status": "ready",
            }

        except Exception as e:
            logger.error(
                f"ClientManager: Failed to process session {self.session_id}: {e}"
            )
            raise

    def _save_batches(self, batches: list) -> None:
        """
        Save all batches to the database in a single transaction.

        Args:
            batches: List of tuples (batch_index, batch_data, labels)
        """
        try:
            with self.db_client:  # Uses context manager for transaction
                inserted = self.repository.insert_batches_bulk(self.session_id, batches)
                logger.info(
                    f"ClientManager: Saved {inserted}/{len(batches)} batches for session {self.session_id}"
                )
        except Exception as e:
            logger.error(
                f"ClientManager: Failed to save batches for session {self.session_id}: {e}"
            )
            raise

    def stop(self) -> None:
        """
        Signal this ClientManager to stop processing.
        This is called during graceful shutdown.
        """
        logger.info(f"ClientManager: Stop requested for session {self.session_id}")
        self.stopped = True


class ClientManagerFactory:
    """
    Factory for creating ClientManager instances.
    This allows for dependency injection and easier testing.
    """

    def __init__(self, config: GlobalConfig, batch_handler: BatchHandler):
        """
        Initialize the factory with shared dependencies.

        Args:
            config: Global configuration
            batch_handler: Shared batch handler instance
        """
        self.config = config
        self.batch_handler = batch_handler

    def create(
        self,
        channel: Any,
        db_client: DatabaseClient,
        session_id: str,
    ) -> ClientManager:
        """
        Create a new ClientManager instance.

        Args:
            channel: RabbitMQ channel
            db_client: Database client
            session_id: Session ID for this client

        Returns:
            A new ClientManager instance
        """
        return ClientManager(
            config=self.config,
            channel=channel,
            db_client=db_client,
            batch_handler=self.batch_handler,
            session_id=session_id,
        )
