import os
from typing import Optional
from dotenv import load_dotenv


DISPATCHER_EXCHANGE = "dispatcher_exchange"
NEW_CONNECTIONS_EXCHANGE = "new_connections_exchange"
CONSUME_QUEUE = "dataset_service_queue"

# Load environment variables from .env file if it exists
load_dotenv()


class DatabaseConfig:
    """PostgreSQL connection configuration"""

    def __init__(self):
        self.host: str = os.getenv("POSTGRES_HOST", "localhost")
        self.port: int = int(os.getenv("POSTGRES_PORT", "5432"))
        self.user: str = os.getenv("POSTGRES_USER", "postgres")
        self.password: str = os.getenv("POSTGRES_PASS", "postgres")
        self.dbname: str = os.getenv("POSTGRES_NAME", "dataset_db")

    def get_connection_string(self) -> str:
        """Returns a connection string for psycopg2"""
        return f"host={self.host} port={self.port} user={self.user} password={self.password} dbname={self.dbname}"


class MiddlewareConfig:
    """RabbitMQ connection configuration"""

    def __init__(self):
        self.host: str = os.getenv("RABBITMQ_HOST", "localhost")
        self.port: int = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.username: str = os.getenv("RABBITMQ_USER", "guest")
        self.password: str = os.getenv("RABBITMQ_PASSWORD", "guest")
        self.max_retries: int = int(os.getenv("RABBITMQ_MAX_RETRIES", "5"))


class GlobalConfig:
    """Global application configuration"""

    def __init__(self):
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.pod_name: str = os.getenv("POD_NAME", "dataset-service")
        self.worker_pool_size: int = int(os.getenv("WORKER_POOL_SIZE", "10"))
        self.batch_size: int = int(os.getenv("BATCH_SIZE", "64"))
        self.batch_commit_size: int = int(os.getenv("BATCH_COMMIT_SIZE", "10"))

        self.middleware_config: MiddlewareConfig = MiddlewareConfig()
        self.database_config: DatabaseConfig = DatabaseConfig()

    def validate(self):
        """Validate configuration values"""
        if self.worker_pool_size < 1:
            raise ValueError("WORKER_POOL_SIZE must be at least 1")

        if self.batch_size < 1:
            raise ValueError("BATCH_SIZE must be at least 1")

        if self.batch_commit_size < 1:
            raise ValueError("BATCH_COMMIT_SIZE must be at least 1")

        if self.middleware_config.max_retries < 0:
            raise ValueError("RABBITMQ_MAX_RETRIES must be non-negative")

        if self.database_config.port < 1 or self.database_config.port > 65535:
            raise ValueError("POSTGRES_PORT must be between 1 and 65535")

        if self.middleware_config.port < 1 or self.middleware_config.port > 65535:
            raise ValueError("RABBITMQ_PORT must be between 1 and 65535")


def load_config() -> GlobalConfig:
    """Load and validate global configuration"""
    config = GlobalConfig()
    config.validate()
    return config
