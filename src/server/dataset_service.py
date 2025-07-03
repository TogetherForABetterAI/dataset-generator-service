from src.pb import dataset_service_pb2, dataset_service_pb2_grpc
from src.data.mnist_loader import load_mnist
from torch.utils.data import DataLoader
import grpc
import numpy as np
from functools import lru_cache


class DatasetServiceServicer(dataset_service_pb2_grpc.DatasetServiceServicer):
    def __init__(self):
        self.datasets = {}
        self.batch_indices = {}  # Cache for batch indices
        self._load_mnist()

    def _load_mnist(self):
        self.datasets["mnist"] = load_mnist()

    @lru_cache(maxsize=128)
    def _get_batch_indices(self, dataset_name, batch_size):
        """Pre-compute batch indices for efficient access with caching"""
        if (dataset_name, batch_size) not in self.batch_indices:
            dataset = self.datasets[dataset_name]
            total_samples = len(dataset)
            num_batches = (total_samples + batch_size - 1) // batch_size

            indices = []
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min(start_idx + batch_size, total_samples)
                indices.append((start_idx, end_idx))

            self.batch_indices[(dataset_name, batch_size)] = indices

        return self.batch_indices[(dataset_name, batch_size)]

    def _extract_batch_efficient(self, dataset, start_idx, end_idx):
        """Efficiently extract batch data using numpy operations"""
        # Use numpy for faster array operations
        batch_size = end_idx - start_idx

        # Get the first sample to determine shape and dtype
        first_sample = dataset[0][0]
        if hasattr(first_sample, "numpy"):
            # Convert PyTorch tensor to numpy
            sample_np = first_sample.numpy()
            sample_shape = sample_np.shape
            sample_dtype = sample_np.dtype
        else:
            # Already numpy array
            sample_shape = first_sample.shape
            sample_dtype = first_sample.dtype

        # Pre-allocate array for better performance
        batch_array = np.empty((batch_size,) + sample_shape, dtype=sample_dtype)

        # Fill array efficiently
        for i in range(batch_size):
            sample = dataset[start_idx + i][0]
            if hasattr(sample, "numpy"):
                # Convert PyTorch tensor to numpy
                batch_array[i] = sample.numpy()
            else:
                # Already numpy array
                batch_array[i] = sample

        return batch_array

    def StreamBatches(self, request, context):
        dataset = self.datasets.get(request.dataset_name)
        if dataset is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Dataset not found")
            return

        batch_indices = self._get_batch_indices(
            request.dataset_name, request.batch_size
        )

        for idx, (start_idx, end_idx) in enumerate(batch_indices):
            # Extract batch efficiently
            batch_tensor = self._extract_batch_efficient(dataset, start_idx, end_idx)
            batch_bytes = batch_tensor.tobytes()

            yield dataset_service_pb2.DataBatch(
                data=batch_bytes,
                batch_index=idx,
                is_last_batch=(idx == len(batch_indices) - 1),
            )

    def GetBatch(self, request, context):
        dataset = self.datasets.get(request.dataset_name)
        if dataset is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Dataset not found")
            return dataset_service_pb2.DataBatch()

        # Get pre-computed batch indices
        batch_indices = self._get_batch_indices(
            request.dataset_name, request.batch_size
        )

        # Check if batch_index is valid
        if request.batch_index >= len(batch_indices):
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Batch index out of range")
            return dataset_service_pb2.DataBatch()

        # Extract batch directly using pre-computed indices
        start_idx, end_idx = batch_indices[request.batch_index]

        # Extract batch efficiently
        batch_tensor = self._extract_batch_efficient(dataset, start_idx, end_idx)
        batch_bytes = batch_tensor.tobytes()

        return dataset_service_pb2.DataBatch(
            data=batch_bytes,
            batch_index=request.batch_index,
            is_last_batch=(request.batch_index == len(batch_indices) - 1),
        )

    def GetDatasetInfo(self, request, context):
        dataset = self.datasets.get(request.dataset_name)
        if dataset is None:
            return dataset_service_pb2.DatasetInfo(
                dataset_name=request.dataset_name,
                total_samples=0,
                sample_shape=[],
                data_type="",
                is_available=False,
            )
        sample = dataset[0][0]
        if hasattr(sample, "numpy"):
            sample_np = sample.numpy()
            sample_shape = list(sample_np.shape)
            sample_dtype = str(sample_np.dtype)
        else:
            sample_shape = list(sample.shape)
            sample_dtype = str(sample.dtype)

        return dataset_service_pb2.DatasetInfo(
            dataset_name=request.dataset_name,
            total_samples=len(dataset),
            sample_shape=sample_shape,
            data_type=sample_dtype,
            is_available=True,
        )

    def HealthCheck(self, request, context):
        return dataset_service_pb2.HealthCheckResponse(
            status="SERVING", message="Service is healthy."
        )
