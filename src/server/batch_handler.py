import logging
from typing import List, Tuple, Dict, Any
import numpy as np

logger = logging.getLogger(__name__)


class BatchHandler:
    """
    Handles the generation of batches from datasets.
    This class encapsulates the logic of extracting and preparing batch data.
    """

    def __init__(self, datasets: Dict[str, Any]):
        """
        Initialize the BatchHandler with loaded datasets.

        Args:
            datasets: Dictionary mapping dataset names to dataset objects
        """
        self.datasets = datasets

    def generate_batches(
        self, dataset_name: str, batch_size: int
    ) -> Tuple[List[Tuple[int, np.ndarray, List[int]]], int]:
        """
        Generate all batches for a given dataset and batch size.

        Args:
            dataset_name: Name of the dataset to use
            batch_size: Size of each batch

        Returns:
            Tuple of (list of batches, total number of batches)
            Each batch is a tuple of (batch_index, batch_data, labels)

        Raises:
            ValueError: If dataset not found
        """
        dataset = self.datasets.get(dataset_name)
        if dataset is None:
            raise ValueError(f"Dataset '{dataset_name}' not found")

        total_samples = len(dataset)
        num_batches = (total_samples + batch_size - 1) // batch_size

        logger.info(
            f"Generating {num_batches} batches from dataset '{dataset_name}' "
            f"(total_samples={total_samples}, batch_size={batch_size})"
        )

        batches = []
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_samples)

            batch_data, labels = self._extract_batch(dataset, start_idx, end_idx)
            batches.append((batch_idx, batch_data, labels))

        logger.debug(f"Generated {len(batches)} batches for dataset '{dataset_name}'")
        return batches, num_batches

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

    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get information about a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dictionary with dataset information

        Raises:
            ValueError: If dataset not found
        """
        dataset = self.datasets.get(dataset_name)
        if dataset is None:
            raise ValueError(f"Dataset '{dataset_name}' not found")

        total_samples = len(dataset)
        first_sample = dataset[0][0]

        if hasattr(first_sample, "numpy"):
            sample_np = first_sample.numpy()
            sample_shape = list(sample_np.shape)
            sample_dtype = str(sample_np.dtype)
        else:
            sample_shape = list(first_sample.shape)
            sample_dtype = str(first_sample.dtype)

        return {
            "dataset_name": dataset_name,
            "total_samples": total_samples,
            "sample_shape": sample_shape,
            "data_type": sample_dtype,
        }
