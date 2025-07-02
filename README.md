# Dataset Generator Service

## Workflow: Generating gRPC Python Code and Running the Service

### 1. Regenerate gRPC Python Code (when the proto changes)

If you update the `.proto` file (e.g., add a new field or method), you need to regenerate the Python gRPC code.  
**Use the proto-gen Dockerfile for this:**

```bash
sudo docker compose up --build proto-gen
```

- This will generate the updated Python files in `src/pb/` and automatically patch the imports for correct package usage.
- You only need to do this when the proto file changes.

---

### 2. Build and Run the gRPC Service

After generating (or if no proto changes), build and run the main service as usual:

```bash
sudo docker compose up --build dataset-grpc-service
```

- This will start the gRPC server, ready to serve requests.

---


### Notes
- The service will automatically restart if it crashes or the system reboots, unless you stop it manually (`restart: unless-stopped`).
---
