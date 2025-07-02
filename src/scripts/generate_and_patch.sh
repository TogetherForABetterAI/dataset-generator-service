#!/bin/sh
set -e

python -m grpc_tools.protoc \
    -I./proto \
    --python_out=/app/out \
    --grpc_python_out=/app/out \
    ./proto/dataset_service.proto

sed -i "s|import dataset_service_pb2 as|from . import dataset_service_pb2 as|" /app/out/dataset_service_pb2_grpc.py