import logging
import signal
import json
import multiprocessing
from typing import Optional
from src.config.config import CONSUME_QUEUE, GlobalConfig
from src.middleware.middleware import Middleware
from src.db.client import DatabaseClient
from src.server.client_manager import ClientManagerFactory
from src.server.batch_handler import BatchHandler
from src.models.notification import ConnectNotification

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):
    """
    Worker process that handles dataset generation jobs.
    Inherits from multiprocessing.Process to handle SIGTERM gracefully.

    Each worker:
    - Consumes directly from RabbitMQ (generate_data_queue)
    - Has its own exclusive RabbitMQ channel
    - Has its own database connection
    - Loads datasets once at initialization
    - Uses the same channel for consume, ACK, and publish (transactional)
    - Handles SIGTERM for graceful shutdown
    """

    def __init__(
        self,
        worker_id: int,
        config: GlobalConfig,
        shared_datasets,
    ):
        """
        Initialize the worker.

        Args:
            worker_id: Unique identifier for this worker
            config: Global configuration
            shared_datasets: Shared datasets in memory (read-only)
        """
        super().__init__()
        self.worker_id = worker_id
        self.config = config
        self.shared_datasets = shared_datasets

        # Shutdown coordination queue for cancelling in-progress work
        self.shutdown_queue = multiprocessing.Queue(maxsize=1)

        # Will be initialized in run() (after fork)
        self.middleware: Optional[Middleware] = None
        self.db_client: Optional[DatabaseClient] = None
        self.client_manager = None
        self.channel = None
        self.consumer_tag: Optional[str] = None

    def _initialize(self):
        """
        Initialize worker resources:
        - RabbitMQ connection and exclusive channel
        - Database connection
        - ClientManager with shared datasets and shutdown queue
        - Setup channel with prefetch_count=1 for fair dispatch
        """
        logger.info(f"Worker {self.worker_id} initializing...")

        # Initialize middleware with exclusive channel
        self.middleware = Middleware(self.config.middleware_config)
        connection = self.middleware.connect()
        self.channel = self.middleware.create_channel(connection)

        # Set QoS for this worker's channel (prefetch_count=1 for fair dispatch)
        self.middleware.set_qos(channel=self.channel, prefetch_count=1)

        # Initialize database client
        self.db_client = DatabaseClient(self.config.database_config)

        # Create batch handler with all dependencies
        batch_handler = BatchHandler(
            shared_datasets=self.shared_datasets,
            shutdown_queue=self.shutdown_queue,
            db_client=self.db_client,
            batch_commit_size=self.config.batch_commit_size,
        )

        # Initialize client manager with batch handler
        self.client_manager = ClientManagerFactory.create(
            batch_size=self.config.batch_size,
            batch_handler=batch_handler,
            middleware=self.middleware,
            channel=self.channel,
        )

        logger.info(f"Worker {self.worker_id} initialization complete")

    def run(self):
        """
        Main worker loop (runs in the child process).

        1. Setup signal handlers for SIGTERM/SIGINT
        2. Initialize connections and datasets
        3. Start consuming from RabbitMQ (blocks until shutdown)
        4. Shutdown gracefully on SIGTERM
        """
        # Setup signal handlers in this process
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        try:
            # Setup worker
            self._initialize()

            # Start consuming from RabbitMQ
            self._start_consuming()

        except KeyboardInterrupt:
            logger.info(f"Worker {self.worker_id} received KeyboardInterrupt")
        except Exception as e:
            logger.error(f"Worker {self.worker_id} fatal error: {e}", exc_info=True)
        finally:
            self._cleanup()

    def _signal_handler(self, signum, frame):
        """
        Handle SIGTERM/SIGINT signals.
        1. Stop consuming new messages from RabbitMQ
        2. Send cancellation signal to in-progress work via shutdown_queue
        The BatchHandler will check this queue and abort processing.
        """
        logger.info(
            f"Worker {self.worker_id} received signal {signum}, shutting down..."
        )

        # Stop consuming new messages
        if self.middleware and self.consumer_tag:
            try:
                self.middleware.stop_consuming(self.consumer_tag)
            except Exception as e:
                logger.warning(f"Worker {self.worker_id} error stopping consumer: {e}")

        # Send cancellation signal to BatchHandler (if it's processing)
        try:
            self.shutdown_queue.put(None, block=False)
        except:
            pass

    def _start_consuming(self):
        """
        Start consuming messages directly from RabbitMQ.
        Uses callback for message processing.
        Blocks until stop_consuming is called (usually via signal handler).
        """

        def callback(ch, method, properties, body):
            """
            RabbitMQ message callback.
            Processes message using same channel for ACK and publish (transactional).
            """
            try:
                # Parse notification
                notification_dict = json.loads(body)

                try:
                    notification = ConnectNotification.from_dict(notification_dict)
                    if not notification.validate():
                        logger.error(
                            f"Worker {self.worker_id}: Notification missing required fields."
                            f"Received notification: {notification_dict}. Rejecting message."
                        )
                        self.middleware.nack_message(
                            channel=ch, delivery_tag=method.delivery_tag, requeue=False
                        )
                        return
                except Exception as e:
                    logger.error(
                        f"Worker {self.worker_id}: Failed to parse notification: {e}, rejecting"
                    )
                    self.middleware.nack_message(
                        channel=ch, delivery_tag=method.delivery_tag, requeue=False
                    )
                    return

                logger.info(
                    f"Worker {self.worker_id} processing: "
                    f"client_id={notification.client_id}, session_id={notification.session_id}"
                )

                # Handle the client request
                # Finally publish_with_transaction (ACK + Publish atomically)
                result = self.client_manager["handle_client"](
                    notification=notification,
                    delivery_tag=method.delivery_tag,
                )

                logger.info(
                    f"Worker {self.worker_id} completed: "
                    f"client_id={notification.client_id}, session_id={notification.session_id}, "
                    f"batches_generated={result.get('batches_generated', 0)}"
                )

            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} error in callback: {e}", exc_info=True
                )
                # NACK the message on error (no requeue to avoid infinite loops)
                try:
                    self.middleware.nack_message(
                        channel=ch, delivery_tag=method.delivery_tag, requeue=False
                    )
                except Exception as nack_error:
                    logger.error(
                        f"Worker {self.worker_id} failed to NACK message: {nack_error}"
                    )

        # Use worker_id as consumer_tag for identification
        consumer_tag = f"{self.config.pod_name}-worker-{self.worker_id}"
        self.consumer_tag = self.middleware.consume(
            channel=self.channel,
            queue=CONSUME_QUEUE,
            callback=callback,
            auto_ack=False,
            consumer_tag=consumer_tag,
        )

        logger.info(
            f"Worker {self.worker_id} consuming from generate_data_queue "
            f"(consumer_tag={self.consumer_tag}, prefetch_count=1)"
        )

        # Start blocking consume loop (blocks until stop_consuming)
        self.middleware.start_consuming(self.channel)

    def _cleanup(self):
        """
        Clean up worker resources:
        - Close RabbitMQ channel and connection
        - Close database connection
        """
        logger.info(f"Worker {self.worker_id} cleaning up...")

        # Close middleware connection
        if self.middleware:
            try:
                self.middleware.close()
                logger.info(f"Worker {self.worker_id} closed RabbitMQ connection")
            except Exception as e:
                logger.warning(f"Worker {self.worker_id} error closing middleware: {e}")

        # Close database connection
        if self.db_client:
            try:
                self.db_client.close()
                logger.info(f"Worker {self.worker_id} closed database connection")
            except Exception as e:
                logger.warning(f"Worker {self.worker_id} error closing database: {e}")

        logger.info(f"Worker {self.worker_id} cleanup complete")
