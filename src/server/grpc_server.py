import grpc
from concurrent import futures
import time
from src.pb import dataset_service_pb2, dataset_service_pb2_grpc
from torchvision import datasets, transforms
from torch.utils.data import DataLoader
import torch
import numpy as np
from src.data.mnist_loader import load_mnist
from grpc_reflection.v1alpha import reflection


# Service Implementation
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
            dataset, batch_size=request.batch_size, shuffle=request.shuffle
        )
        for idx, (data, target) in enumerate(loader):
            batch_np = data.numpy()
            batch_bytes = batch_np.tobytes()
            yield dataset_service_pb2.DataBatch(
                data=batch_bytes,
                batch_size=data.shape[0],
                batch_index=idx,
                is_last_batch=(idx == len(loader) - 1),
                data_format="numpy_array",
                data_shape=list(data.shape),
            )

    def GetBatch(self, request, context):
        dataset = self.datasets.get(request.dataset_name)
        if dataset is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Dataset not found")
            return dataset_service_pb2.DataBatch()
        loader = DataLoader(
            dataset, batch_size=request.batch_size, shuffle=request.shuffle
        )
        data, target = next(iter(loader))
        batch_np = data.numpy()
        batch_bytes = batch_np.tobytes()
        return dataset_service_pb2.DataBatch(
            data=batch_bytes,
            batch_size=data.shape[0],
            batch_index=0,
            is_last_batch=True,
            data_format="numpy_array",
            data_shape=list(data.shape),
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


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dataset_service_pb2_grpc.add_DatasetServiceServicer_to_server(
        DatasetServiceServicer(), server
    )
    SERVICE_NAMES = (
        dataset_service_pb2.DESCRIPTOR.services_by_name["DatasetService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port("[::]:50051")
    print("gRPC server started on port 50051.")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
