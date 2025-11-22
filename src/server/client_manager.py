import logging
from typing import Dict, Any, Optional
from src.config.config import DISPATCHER_EXCHANGE, GlobalConfig
from src.middleware.middleware import RabbitMQMiddleware
from src.db.client import DatabaseClient
from src.server.batch_handler import BatchHandler
from src.models.notify_dispatcher import NotifyDispatcher

logger = logging.getLogger(__name__)


class ClientManagerFactory:
    """
    Factory for creating ClientManager functionality with all dependencies.
    Creates batch handler and provides handle_client function.
    """

    @staticmethod
    def create(
        batch_size: int,
        batch_commit_size: int,
        middleware: RabbitMQMiddleware,
        db_client: DatabaseClient,
        channel: Any,
        shutdown_queue: Any,
        shared_datasets=None,
    ):
        """
        Create a ClientManager with all necessary dependencies.

        Args:
            batch_size: Size of each batch
            batch_commit_size: Number of batches to accumulate before committing to DB
            middleware: RabbitMQ middleware instance
            db_client: Database client instance
            channel: RabbitMQ channel
            shutdown_queue: Queue to signal work cancellation
            shared_datasets: SharedDatasets object with read-only datasets

        Returns:
            Dictionary with handle_client function
        """
        # Create batch handler with shared datasets
        batch_handler = BatchHandler(
            shared_datasets=shared_datasets,
            shutdown_queue=shutdown_queue,
            db_client=db_client,
            batch_commit_size=batch_commit_size,
        )

        return {
            "handle_client": lambda notification, delivery_tag: ClientManagerFactory._handle_client(
                notification=notification,
                delivery_tag=delivery_tag,
                batch_size=batch_size,
                middleware=middleware,
                channel=channel,
                batch_handler=batch_handler,
            )
        }

    @staticmethod
    def _handle_client(
        notification,
        delivery_tag: int,
        batch_size: int,
        middleware: RabbitMQMiddleware,
        channel: Any,
        batch_handler: BatchHandler,
    ) -> Dict[str, Any]:
        """
        Handle a client request: generate batches and save to DB (done by batch_handler),
        then publish response and ACK.

        Args:
            notification: ConnectNotification with session_id, model_type (ACDC/MNIST), etc.
            delivery_tag: RabbitMQ delivery tag for ACK
            batch_size: Batch size from config
            middleware: RabbitMQ middleware
            channel: RabbitMQ channel
            batch_handler: Batch handler that generates and saves batches

        Returns:
            Dictionary with processing results
        """
        session_id = notification.session_id
        model_type = notification.model_type

        if not model_type:
            raise ValueError("model_type is required in notification")

        logger.info(
            f"ClientManager: Processing session_id={session_id}, "
            f"model_type={model_type}, batch_size={batch_size}"
        )

        # Generate and save batches incrementally
        total_batches = batch_handler.generate_batches(
            session_id=session_id, model_type=model_type, batch_size=batch_size
        )

        # Check if generation was cancelled
        if total_batches is None:
            logger.warning(f"Batch generation cancelled for session {session_id}")
            # NACK message on cancellation (requeue for retry)
            middleware.nack_message(
                channel=channel, delivery_tag=delivery_tag, requeue=True
            )
            return {"status": "cancelled"}

        # Create dispatcher notification
        notify = NotifyDispatcher(
            client_id=notification.client_id,
            session_id=session_id,
            total_batches_generated=total_batches,
        )

        # Publish response to dispatcher with transactional ACK
        # This ensures atomicity: both ACK and Publish succeed or both fail
        middleware.publish_with_transaction(
            channel=channel,
            exchange=DISPATCHER_EXCHANGE,
            routing_key="",
            message=notify.to_dict(),  # Pass dict, not JSON string
            delivery_tag=delivery_tag,
        )

        logger.info(f"Successfully completed session {session_id}")
        return {"status": "completed", "batches_generated": total_batches}
