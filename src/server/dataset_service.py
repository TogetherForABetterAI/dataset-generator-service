from src.pb import dataset_service_pb2, dataset_service_pb2_grpc
from src.data.mnist_loader import load_mnist
from torch.utils.data import DataLoader
import grpc


class DatasetServiceServicer(dataset_service_pb2_grpc.DatasetServiceServicer):
    def __init__(self):
        self.datasets = {}
        self._load_mnist()

    def _load_mnist(self):
        self.datasets["mnist"] = load_mnist()

    def StreamBatches(self, request, context):
        dataset = self.datasets.get(request.dataset_name)
        if dataset is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Dataset not found")
            return
        loader = DataLoader(
            dataset, batch_size=request.batch_size)
        for idx, (data, target) in enumerate(loader):
            batch_np = data.numpy()
            batch_bytes = batch_np.tobytes()
            yield dataset_service_pb2.DataBatch(
                data=batch_bytes,
                batch_index=idx,
                is_last_batch=(idx == len(loader) - 1),
            )

    def GetBatch(self, request, context):
            dataset = self.datasets.get(request.dataset_name)
            if dataset is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Dataset not found")
                return dataset_service_pb2.DataBatch()
            loader = DataLoader(
                dataset, batch_size=request.batch_size)
            # Get the batch at the requested index
            for idx, (data, target) in enumerate(loader):
                if idx == request.batch_index:
                    batch_np = data.numpy()
                    batch_bytes = batch_np.tobytes()
                    is_last = (idx == len(loader) - 1)
                    return dataset_service_pb2.DataBatch(
                        data=batch_bytes,
                        batch_index=idx,
                        is_last_batch=is_last,
                    )
            # If batch_index is out of range
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Batch index out of range")
            return dataset_service_pb2.DataBatch()

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
        return dataset_service_pb2.DatasetInfo(
            dataset_name=request.dataset_name,
            total_samples=len(dataset),
            sample_shape=list(sample.shape),
            data_type=str(sample.dtype),
            is_available=True,
        )

    def HealthCheck(self, request, context):
        return dataset_service_pb2.HealthCheckResponse(
            status="SERVING", message="Service is healthy."
        )
