"""
Shared memory datasets for multiprocessing workers.

Datasets are loaded ONCE in the parent process and stored in shared memory.
Workers receive the SharedDatasets object and read from it (read-only).
"""

import logging
import multiprocessing
import numpy as np
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class SharedDatasets:
    """
    Simple container for datasets in shared memory.

    Usage:
        # Parent process:
        shared_datasets = SharedDatasets()
        shared_datasets.add_dataset('mnist', load_mnist)

        # Pass to workers, they read directly:
        data, labels = shared_datasets.get_dataset('mnist')
        sample, label = data[idx], labels[idx]
    """

    def __init__(self):
        """Initialize empty container."""
        self._data: Dict[str, np.ndarray] = {}
        self._labels: Dict[str, np.ndarray] = {}
        self._num_samples: Dict[str, int] = {}

    def add_dataset(self, name: str, loader_func) -> None:
        """
        Load a dataset into shared memory.

        Args:
            name: Dataset name (e.g., 'mnist', 'acdc')
            loader_func: Function that returns the PyTorch dataset
        """
        logger.info(f"Loading dataset '{name}' into shared memory...")

        # Load dataset
        dataset = loader_func()
        total_samples = len(dataset)
        logger.info(f"Dataset '{name}' has {total_samples} samples")

        # Get first sample to determine shape
        first_sample, first_label = dataset[0]
        if hasattr(first_sample, "numpy"):
            sample_array = first_sample.numpy()
        else:
            sample_array = np.array(first_sample)

        data_shape = sample_array.shape

        # Create shared memory arrays
        data_size = (total_samples,) + data_shape
        shared_data = multiprocessing.Array("f", int(np.prod(data_size)), lock=False)
        data_np = np.frombuffer(shared_data, dtype=np.float32).reshape(data_size)

        shared_labels = multiprocessing.Array("i", total_samples, lock=False)
        labels_np = np.frombuffer(shared_labels, dtype=np.int32)

        # Copy data to shared memory
        logger.info(f"Copying {total_samples} samples to shared memory...")
        for i in range(total_samples):
            sample, label = dataset[i]

            if hasattr(sample, "numpy"):
                sample_np = sample.numpy()
            else:
                sample_np = np.array(sample)

            data_np[i] = sample_np.astype(np.float32)
            labels_np[i] = int(label)

        # Make read-only
        data_np.setflags(write=False)
        labels_np.setflags(write=False)

        # Store
        self._data[name] = data_np
        self._labels[name] = labels_np
        self._num_samples[name] = total_samples

        logger.info(
            f"Dataset '{name}' loaded: {total_samples} samples, "
            f"shape={data_shape}, memory={data_np.nbytes / 1024 / 1024:.2f} MB"
        )

    def get_dataset(self, name: str) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get dataset arrays (read-only).

        Args:
            name: Dataset name (case-insensitive)

        Returns:
            Tuple of (data_array, labels_array) - both read-only numpy arrays

        Raises:
            ValueError: If dataset not found
        """
        name_lower = name.lower()

        if name_lower not in self._data:
            available = list(self._data.keys())
            raise ValueError(f"Dataset '{name}' not found. Available: {available}")

        return self._data[name_lower], self._labels[name_lower]

    def get_num_samples(self, name: str) -> int:
        """Get number of samples in a dataset."""
        name_lower = name.lower()
        if name_lower not in self._num_samples:
            raise ValueError(f"Dataset '{name}' not found")
        return self._num_samples[name_lower]

    def get_available_datasets(self) -> list:
        """Get list of available dataset names."""
        return list(self._data.keys())
