# Dataset Generator Service

## Workflow: Generating gRPC Python Code and Running the Service

### 1. Regenerate gRPC Python Code (when the proto changes)

If you update the `.proto` file (e.g., add a new field or method), you need to regenerate the Python gRPC code. Use the provided script:

```bash
chmod +x src/scripts/gen_proto.sh  # Run this once to make the script executable
sudo src/scripts/gen_proto.sh
```

- This script will delete the old generated code in `src/pb/`, recreate the folder, and run the proto-gen container to generate the new code.
- You only need to do this when the proto file changes.

---

### 2. Build and Run the gRPC Service

After generating (or if no proto changes), build and run the main service as usual:

```bash
docker compose up --build dataset-grpc-service
```

- This will start the gRPC server, ready to serve requests.

---

### Example: Query a Batch with grpcurl

You can test the service using [grpcurl](https://github.com/fullstorydev/grpcurl). For example, to request the last batch of the MNIST dataset (with batch size 2):

```bash
grpcurl -plaintext -emit-defaults -d '{"dataset_name": "mnist", "batch_size": 2, "batch_index": 4999}' localhost:50051 dataset_service.DatasetService/GetBatch
```

This will return a response like:

```json
{
  "data": "...binary...",
  "batch_index": 4999,
  "is_last_batch": true
}
```

