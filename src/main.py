import grpc
from concurrent import futures
import time
import os
from grpc_reflection.v1alpha import reflection
from src.pb import dataset_service_pb2, dataset_service_pb2_grpc
from src.server.dataset_service import DatasetServiceServicer


def serve():
    # Optimize thread pool size based on CPU cores
    max_workers = min(32, (os.cpu_count() or 1) * 4)

    # Configure server with optimized settings
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=[
            ("grpc.max_send_message_length", 100 * 1024 * 1024),  # 100MB
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),  # 100MB
            ("grpc.max_concurrent_streams", 100),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.keepalive_permit_without_calls", True),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.http2.min_time_between_pings_ms", 10000),
            ("grpc.http2.min_ping_interval_without_data_ms", 300000),
        ],
    )

    dataset_service_pb2_grpc.add_DatasetServiceServicer_to_server(
        DatasetServiceServicer(), server
    )

    SERVICE_NAMES = (
        dataset_service_pb2.DESCRIPTOR.services_by_name["DatasetService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:50051")
    print(f"gRPC server started on port 50051 with {max_workers} workers.")
    server.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.stop(0)


if __name__ == "__main__":
    serve()
