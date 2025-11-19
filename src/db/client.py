import psycopg2
import psycopg2.extras
import logging
from typing import Optional
from src.config.config import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    PostgreSQL database client using psycopg2.
    Manages connection pooling and provides a context manager for transactions.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection: Optional[psycopg2.extensions.connection] = None

    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            logger.info(
                f"Connecting to PostgreSQL at {self.config.host}:{self.config.port}/{self.config.dbname}"
            )
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                dbname=self.config.dbname,
            )
            # Set autocommit to False for explicit transaction management
            self.connection.autocommit = False
            logger.info("Successfully connected to PostgreSQL")

            # Initialize the schema
            self._initialize_schema()

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _initialize_schema(self):
        """Create the batches table if it doesn't exist"""
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS batches (
                    session_id VARCHAR(255) NOT NULL,
                    batch_index INTEGER NOT NULL,
                    data_payload BYTEA NOT NULL,
                    labels JSON NOT NULL,
                    isEnqueued BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (session_id, batch_index)
                );
                
                CREATE INDEX IF NOT EXISTS idx_batches_session_id 
                ON batches(session_id);
                
                CREATE INDEX IF NOT EXISTS idx_batches_isenqueued 
                ON batches(isEnqueued);
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("Database schema initialized successfully")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to initialize schema: {e}")
            raise

    def get_cursor(self):
        """Get a database cursor for executing queries"""
        if self.connection is None or self.connection.closed:
            self.connect()
        return self.connection.cursor()

    def commit(self):
        """Commit the current transaction"""
        if self.connection:
            self.connection.commit()
            logger.debug("Transaction committed")

    def rollback(self):
        """Rollback the current transaction"""
        if self.connection:
            self.connection.rollback()
            logger.warning("Transaction rolled back")

    def close(self):
        """Close the database connection"""
        if self.connection and not self.connection.closed:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

    def is_connected(self) -> bool:
        """Check if the connection is active"""
        return self.connection is not None and not self.connection.closed

    def __enter__(self):
        """Context manager entry"""
        if not self.is_connected():
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic rollback on exception"""
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        return False
