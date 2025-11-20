import logging
from typing import List, Tuple, Dict, Any, Optional
import numpy as np
import queue
from src.repository.batch_repository import BatchRepository

logger = logging.getLogger(__name__)


class BatchHandler:
    """
    Handles the generation of batches from datasets.
    This class encapsulates the logic of extracting and preparing batch data.
    Supports cancellation via shutdown_queue.
    """

    def __init__(
        self,
        shared_datasets,
        db_client,
        batch_commit_size: int,
        shutdown_queue: queue.Queue,
    ):
        """
        Initialize the BatchHandler.

        Args:
            shared_datasets: SharedDatasets object with read-only datasets
            db_client: Database client for transactions
            batch_commit_size: Number of batches to accumulate before committing to DB
            shutdown_queue: Queue to check for cancellation signals (None in queue = cancel)
        """
        self.shared_datasets = shared_datasets
        self.shutdown_queue = shutdown_queue
        self.db_client = db_client
        self.batch_commit_size = batch_commit_size

        # Create repository if db_client is provided
        self.repository = None
        if db_client:
            self.repository = BatchRepository(db_client)

    def generate_batches(
        self, session_id: str, model_type: str, batch_size: int
    ) -> Optional[int]:
        """
        Generate all batches for a given dataset and save them incrementally to DB.
        Reads from shared memory datasets (read-only).

        Args:
            session_id: Session ID to associate with the batches
            model_type: Model type (e.g., "MNIST", "ACDC")
            batch_size: Size of each batch

        Returns:
            Total number of batches generated, or None if cancelled

        Raises:
            ValueError: If dataset not found
        """
        # Get dataset from shared memory (read-only)
        try:
            data, labels = self.shared_datasets.get_dataset(model_type)
            total_samples = len(data)
        except ValueError as e:
            logger.error(f"Failed to access dataset: {e}")
            raise

        num_batches = (total_samples + batch_size - 1) // batch_size

        logger.info(
            f"Generating {num_batches} batches for '{model_type}' "
            f"({total_samples} samples, batch_size={batch_size}, "
            f"commit_size={self.batch_commit_size}) from shared memory"
        )

        batch_accumulator = []
        total_saved = 0

        for batch_idx in range(num_batches):
            # Check for cancellation
            if self._check_cancelled():
                logger.warning(
                    f"Cancelled at batch {batch_idx}/{num_batches}, saved {total_saved}"
                )
                return None

            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_samples)

            # Extract batch from shared memory
            batch_data = data[start_idx:end_idx]
            batch_labels = labels[start_idx:end_idx].tolist()

            batch_accumulator.append((batch_idx, batch_data, batch_labels))

            # Commit when reaching commit_size
            if len(batch_accumulator) >= self.batch_commit_size:
                saved = self._save_batches(session_id, batch_accumulator)
                total_saved += saved
                logger.debug(
                    f"Committed {saved} batches (total: {total_saved}/{num_batches})"
                )
                batch_accumulator = []

        # Save remaining batches
        if len(batch_accumulator) > 0:
            saved = self._save_batches(session_id, batch_accumulator)
            total_saved += saved
            logger.debug(f"Committed final {saved} batches (total: {total_saved})")

        logger.info(f"Completed {total_saved} batches for session {session_id}")
        return total_saved

    def _save_batches(self, session_id: str, batches: List[Tuple]) -> int:
        """
        Save a list of batches to the database in a single transaction.

        Args:
            session_id: Session ID
            batches: List of tuples (batch_index, batch_data, labels)

        Returns:
            Number of batches saved
        """
        if not self.db_client or not self.repository:
            logger.warning("No DB client or repository configured, skipping save")
            return 0

        try:
            # Repository handles the session and transaction
            inserted = self.repository.insert_batches_bulk(session_id, batches)
            return inserted
        except Exception as e:
            logger.error(f"Failed to save {len(batches)} batches: {e}", exc_info=True)
            raise

    def _check_cancelled(self) -> bool:
        """
        Check if cancellation signal has been received.

        Returns:
            True if cancelled (None in queue), False otherwise
        """

        try:
            # Non-blocking check
            signal = self.shutdown_queue.get(block=False)
            if signal is None:
                return True
        except queue.Empty:
            # No signal, continue
            return False

        return False
