# Dataset Generator Service

## Running with Docker Compose

To build and start the gRPC microservice using Docker Compose:

```bash
sudo docker compose up --build
```

This will build the Docker image (if needed) and start the service, exposing gRPC on port 50051.

To stop the service:

```bash
sudo docker compose down
```

### Notes
- The service will automatically restart if it crashes or the system reboots, unless you stop it manually (`restart: unless-stopped`).
---
