import grpc
from concurrent import futures
import time
from grpc_reflection.v1alpha import reflection
from src.pb import dataset_service_pb2, dataset_service_pb2_grpc
from src.server.dataset_service import DatasetServiceServicer


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
