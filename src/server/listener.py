import logging
import multiprocessing
from typing import Optional
from src.config.config import GlobalConfig
from src.server.worker import Worker
from src.middleware.middleware import RabbitMQMiddleware

logger = logging.getLogger(__name__)


class Listener:
    """
    Main listener class that manages the worker pool lifecycle.

    Each worker consumes directly from RabbitMQ with prefetch_count=1,
    allowing RabbitMQ to distribute messages fairly across the pool.

    The Listener's responsibilities:
    - Declare RabbitMQ topology once (exchanges, queues, bindings)
    - Start worker processes
    - Monitor worker health
    - Gracefully shutdown workers on SIGTERM
    """

    def __init__(self, config: GlobalConfig, shared_datasets=None):
        self.config = config
        self.workers = []
        self.shared_datasets = shared_datasets

    def start(self):
        """
        Start the listener with worker pool.

        1. Declare RabbitMQ topology once (idempotent)
        2. Start worker processes (each consumes directly from RabbitMQ)
        """
        logger.info(
            f"Starting Listener with {self.config.worker_pool_size} worker processes"
        )

        try:
            # Declare topology once before starting workers (idempotent)
            logger.info("Declaring RabbitMQ topology...")
            middleware = RabbitMQMiddleware(self.config.middleware_config)
            connection = middleware.connect()
            channel = middleware.create_channel(connection)
            middleware.declare_topology(channel=channel)
            middleware.close()
            logger.info("RabbitMQ topology declared successfully")

            # Create and start worker processes
            # Each worker consumes directly from RabbitMQ with prefetch_count=1
            for i in range(self.config.worker_pool_size):
                worker = Worker(i, self.config, self.shared_datasets)
                worker.start()
                self.workers.append(worker)
                logger.info(f"Started worker process {i} (PID: {worker.pid})")

            logger.info(
                f"Worker pool started. Each worker consuming from generate_data_queue with prefetch_count=1"
            )

            # Wait for all workers to finish (blocks until workers terminate)
            self._wait_for_workers()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
            self.stop()
        except Exception as e:
            logger.error(f"Error in listener: {e}", exc_info=True)
            self.stop()
            raise

    def _wait_for_workers(self):
        """
        Wait for all worker processes to complete.
        This blocks until all workers have terminated.
        """
        for i, worker in enumerate(self.workers):
            worker.join()
            logger.info(f"Worker {i} (PID: {worker.pid}) has terminated")

        logger.info("All workers have terminated")

    def stop(self):
        """
        Gracefully stop all workers.
        Sends SIGTERM to each worker, allowing them to:
        1. Stop consuming new messages
        2. Finish current work
        3. Clean up resources
        """
        logger.info("Sending SIGTERM to all workers...")
        for i, worker in enumerate(self.workers):
            if worker.is_alive():
                worker.terminate()
                logger.info(f"Sent SIGTERM to Worker {i} (PID: {worker.pid})")

        # Wait for workers to finish
        logger.info("Waiting for workers to finish...")
        for i, worker in enumerate(self.workers):
            worker.join()
            logger.info(f"Worker {i} stopped (PID: {worker.pid})")
        logger.info("Listener stopped successfully")
