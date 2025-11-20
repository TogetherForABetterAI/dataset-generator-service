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
        datasets: Dict[str, Any],
        db_client,
        batch_commit_size: int,
        shutdown_queue: queue.Queue,
    ):
        """
        Initialize the BatchHandler.

        Args:
            datasets: Dictionary mapping dataset names to dataset objects
            db_client: Database client for transactions
            batch_commit_size: Number of batches to accumulate before committing to DB
            shutdown_queue: Queue to check for cancellation signals (None in queue = cancel)
        """
        self.datasets = datasets
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
        Saves every N batches (batch_commit_size) to avoid large transactions.
        Checks shutdown_queue periodically and cancels if signal received.

        Args:
            session_id: Session ID to associate with the batches
            model_type: Model type (e.g., "MNIST", "ACDC")
            batch_size: Size of each batch

        Returns:
            Total number of batches generated, or None if cancelled

        Raises:
            ValueError: If dataset not found
        """
        # Lookup dataset by model_type (case-insensitive)
        dataset_key = model_type.lower()
        dataset = self.datasets.get(dataset_key)
        if dataset is None:
            raise ValueError(f"Dataset for model_type '{model_type}' not found")

        total_samples = len(dataset)
        num_batches = (total_samples + batch_size - 1) // batch_size

        logger.info(
            f"Generating {num_batches} batches for model_type '{model_type}' "
            f"(total_samples={total_samples}, batch_size={batch_size}, "
            f"commit_size={self.batch_commit_size})"
        )

        batch_accumulator = []
        total_saved = 0

        for batch_idx in range(num_batches):
            # Check for cancellation signal before processing each batch
            if self._check_cancelled():
                logger.warning(
                    f"Batch generation cancelled at batch {batch_idx}/{num_batches}, "
                    f"saved {total_saved} batches so far"
                )
                return None

            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_samples)

            batch_data, labels = self._extract_batch(dataset, start_idx, end_idx)
            batch_accumulator.append((batch_idx, batch_data, labels))

            # Commit to DB when accumulator reaches commit_size
            if len(batch_accumulator) >= self.batch_commit_size:
                saved = self._save_batches(session_id, batch_accumulator)
                total_saved += saved
                logger.debug(
                    f"Committed {saved} batches to DB (total: {total_saved}/{num_batches})"
                )
                batch_accumulator = []  # Clear accumulator

        # Save remaining batches (if any)
        if len(batch_accumulator) > 0:
            saved = self._save_batches(session_id, batch_accumulator)
            total_saved += saved
            logger.debug(
                f"Committed final {saved} batches to DB (total: {total_saved}/{num_batches})"
            )

        logger.info(
            f"Successfully generated and saved {total_saved} batches for session {session_id}"
        )
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

    def _extract_batch(
        self, dataset: Any, start_idx: int, end_idx: int
    ) -> Tuple[np.ndarray, List[int]]:
        """
        Extract a single batch of data from the dataset.

        Args:
            dataset: The dataset to extract from
            start_idx: Starting index (inclusive)
            end_idx: Ending index (exclusive)

        Returns:
            Tuple of (batch_data as numpy array, labels as list)
        """
        batch_size = end_idx - start_idx

        # Get first sample to determine shape and dtype
        first_sample = dataset[start_idx][0]
        if hasattr(first_sample, "numpy"):
            sample_np = first_sample.numpy()
            sample_shape = sample_np.shape
            sample_dtype = sample_np.dtype
        else:
            sample_shape = first_sample.shape
            sample_dtype = first_sample.dtype

        # Pre-allocate arrays for better performance
        batch_array = np.empty((batch_size,) + sample_shape, dtype=sample_dtype)
        labels_array = []

        # Fill arrays efficiently
        for i in range(batch_size):
            sample, label = dataset[start_idx + i]
            batch_array[i] = sample.numpy() if hasattr(sample, "numpy") else sample
            labels_array.append(int(label))

        return batch_array, labels_array
