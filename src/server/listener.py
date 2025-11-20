import logging
import json
import multiprocessing
from typing import Optional
from src.config.config import GlobalConfig
from src.middleware.middleware import RabbitMQMiddleware
from src.models.notification import ConnectNotification
from src.server.worker import Worker

logger = logging.getLogger(__name__)


class Listener:
    """
    Main listener class that manages the worker pool and message distribution.
    Workers remain idle until they receive work from the jobs queue.
    """

    def __init__(self, config: GlobalConfig):
        self.config = config
        self.middleware = RabbitMQMiddleware(config.middleware_config)
        self.workers = []
        self.jobs_queue = multiprocessing.Queue(maxsize=config.worker_pool_size + 10)
        self.consumer_tag = None  # Will be set when consuming starts

    def start(self):
        """Start the listener with worker pool"""
        logger.info(
            f"Starting Listener with {self.config.worker_pool_size} worker processes"
        )

        try:
            # Connect to RabbitMQ (main process connection)
            connection = self.middleware.connect()
            channel = self.middleware.create_channel(connection)

            # Setup topology (exchange, queue, binding)
            self.middleware.declare_topology(
                channel, prefetch_count=self.config.worker_pool_size
            )

            # Create and start worker processes
            for i in range(self.config.worker_pool_size):
                worker = Worker(i, self.config, self.jobs_queue)
                worker.start()
                self.workers.append(worker)
                logger.info(f"Started worker process {i} (PID: {worker.pid})")

            logger.info("Worker pool started. Workers are idle, waiting for jobs.")

            # Start consuming messages
            self._consume_messages(channel)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in listener: {e}", exc_info=True)
        finally:
            self.stop()

    def _consume_messages(self, channel):
        """
        Consume messages from RabbitMQ and distribute to idle workers via jobs queue.
        When a message arrives, it's placed in the jobs_queue and an idle worker picks it up.
        """

        def callback(ch, method, properties, body):
            try:
                # Parse notification
                notification_dict = json.loads(body)

                # Quick validation using the model
                try:
                    notification = ConnectNotification.from_dict(notification_dict)
                    if not notification.validate():
                        logger.error("Notification missing required fields, rejecting")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        return
                except Exception as e:
                    logger.error(f"Failed to parse notification: {e}, rejecting")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    return

                logger.debug(
                    f"Received notification for client_id={notification.client_id}, "
                    f"session_id={notification.session_id}, adding to jobs queue"
                )

                # Create job and add to queue
                # An idle worker will pick this up
                job = {
                    "notification": notification_dict,  # Pass as dict to worker
                    "delivery_tag": method.delivery_tag,
                }
                self.jobs_queue.put(job)

            except Exception as e:
                logger.error(f"Error in message callback: {e}", exc_info=True)
                # NACK message on parsing error
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        # Start consuming with manual ACK
        # ACK will be done by workers after successful processing
        # Use POD_NAME as consumer_tag for Kubernetes pod identification
        self.consumer_tag = self.middleware.consume(
            channel=channel,
            queue="generate_data_queue",
            callback=callback,
            auto_ack=False,
            consumer_tag=self.config.pod_name,
        )

        logger.info(
            f"Listening for messages on generate_data_queue "
            f"(consumer_tag={self.consumer_tag})..."
        )

        # Start blocking consume loop
        self.middleware.start_consuming(channel)

    def stop(self):
        """
        Interrupt all active workers and stop the listener.
        This is called during graceful shutdown to signal workers to stop.
        """
        self.middleware.stop_consuming(self.consumer_tag)
        self._interrupt_workers()
        self.middleware.close()
        logger.info("Listener stopped successfully")

    def _interrupt_workers(self):
        """
        Interrupt all active workers.
        This is called during graceful shutdown to signal workers to stop.
        """
        logger.info("Interrupting all workers and listener...")

        # If there is any worker without ongoing processing,
        # signal them to stop
        logger.info("Sending stop signals to workers...")
        for _ in range(len(self.workers)):
            self.jobs_queue.put(None)

        # Notify workers to stop in case they are processing
        logger.info("Waiting for workers to finish...")
        for i, worker in enumerate(self.workers):
            worker.terminate()
            logger.info(f"Sent SIGTERM to Worker {i} (PID: {worker.pid})")

        # Wait for workers to finish
        for i, worker in enumerate(self.workers):
            worker.join()
            logger.info(f"Worker {i} stopped (PID: {worker.pid})")
