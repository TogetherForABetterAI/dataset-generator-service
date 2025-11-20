import logging
import signal
import multiprocessing
from typing import Optional
from src.config.config import GlobalConfig
from src.middleware.middleware import RabbitMQMiddleware
from src.db.client import DatabaseClient
from src.server.client_manager import ClientManagerFactory
from src.models.notification import ConnectNotification

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):
    """
    Worker process that handles dataset generation jobs.
    Inherits from multiprocessing.Process to handle SIGTERM gracefully.

    Each worker:
    - Has its own exclusive RabbitMQ channel
    - Has its own database connection
    - Loads datasets once at initialization
    - Processes jobs from the jobs queue
    - Handles SIGTERM for graceful shutdown
    """

    def __init__(
        self, worker_id: int, config: GlobalConfig, jobs_queue: multiprocessing.Queue
    ):
        """
        Initialize the worker.

        Args:
            worker_id: Unique identifier for this worker
            config: Global configuration
            jobs_queue: Queue to receive jobs from the listener
        """
        super().__init__()
        self.worker_id = worker_id
        self.config = config
        self.jobs_queue = jobs_queue

        # Shutdown coordination queue for cancelling in-progress work
        self.shutdown_queue = multiprocessing.Queue(maxsize=1)

        # Will be initialized in run() (after fork)
        self.middleware: Optional[RabbitMQMiddleware] = None
        self.db_client: Optional[DatabaseClient] = None
        self.client_manager = None
        self.channel = None

    def _initialize(self):
        """
        Initialize worker resources:
        - RabbitMQ connection and exclusive channel
        - Database connection
        - ClientManager with dataset loading and shutdown queue
        """
        logger.info(f"Worker {self.worker_id} initializing...")

        # Initialize middleware with exclusive channel
        self.middleware = RabbitMQMiddleware(self.config.middleware_config)
        connection = self.middleware.connect()
        self.channel = self.middleware.create_channel(connection)

        # Initialize database client
        self.db_client = DatabaseClient(self.config.database_config)

        # Initialize client manager (pass shutdown_queue for cancellation)
        self.client_manager = ClientManagerFactory.create(
            batch_size=self.config.batch_size,
            middleware=self.middleware,
            db_client=self.db_client,
            channel=self.channel,
            shutdown_queue=self.shutdown_queue,
        )

        logger.info(f"Worker {self.worker_id} initialization complete")

    def run(self):
        """
        Main worker loop (runs in the child process).

        1. Setup signal handlers for SIGTERM/SIGINT
        2. Initialize connections and datasets
        3. Process jobs from the queue (blocks until job arrives)
        4. Shutdown gracefully on poison pill (None)
        """
        # Setup signal handlers in this process
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        try:
            # Setup worker
            self._initialize()

            # Main job processing loop
            while True:
                try:
                    # Wait for job
                    job = self.jobs_queue.get(block=True)

                    # None = shutdown signal
                    if job is None:
                        logger.info(f"Worker {self.worker_id} received shutdown signal")
                        break

                    # Process the job
                    self._process_job(job)

                except Exception as e:
                    logger.error(
                        f"Worker {self.worker_id} error processing job: {e}",
                        exc_info=True,
                    )
                    # Continue processing other jobs
                    continue

        except Exception as e:
            logger.error(f"Worker {self.worker_id} fatal error: {e}", exc_info=True)
        finally:
            self._cleanup()
            logger.info(f"Worker {self.worker_id} stopped")

    def _signal_handler(self, signum, frame):
        """
        Handle SIGTERM/SIGINT signals.
        Sends cancellation signal to in-progress work via shutdown_queue.
        The BatchHandler will check this queue and abort processing.

        """
        # Send cancellation signal to BatchHandler (if it's processing)
        try:
            self.shutdown_queue.put(None, block=False)
        except:
            pass

    def _process_job(self, job: dict):
        """
        Process a single job from the queue.

        Args:
            job: Dictionary containing 'notification' and 'delivery_tag'
        """
        try:
            notification_dict = job["notification"]
            delivery_tag = job["delivery_tag"]

            # Parse notification
            notification = ConnectNotification.from_dict(notification_dict)

            logger.info(
                f"Worker {self.worker_id} processing job: "
                f"client_id={notification.client_id}, session_id={notification.session_id}"
            )

            # Handle the client request
            result = self.client_manager.handle_client(
                notification=notification,
                delivery_tag=delivery_tag,
            )

            logger.info(
                f"Worker {self.worker_id} completed job: "
                f"client_id={notification.client_id}, session_id={notification.session_id}, "
                f"batches_generated={result.get('batches_generated', 0)}"
            )

        except Exception as e:
            logger.error(
                f"Worker {self.worker_id} error processing job: {e}", exc_info=True
            )
            # NACK the message on error
            try:
                self.middleware.nack_message(
                    channel=self.channel, delivery_tag=delivery_tag, requeue=False
                )
            except Exception as nack_error:
                logger.error(
                    f"Worker {self.worker_id} failed to NACK message: {nack_error}"
                )

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
