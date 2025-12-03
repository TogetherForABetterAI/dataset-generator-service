import logging
from typing import Optional
from sqlalchemy import QueuePool, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from src.config.config import DatabaseConfig
from src.models.batch import Base

logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    PostgreSQL database client using SQLAlchemy.
    Each worker process should create its own DatabaseClient instance.
    Uses NullPool to avoid connection sharing issues in multiprocessing.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = None
        self.SessionLocal = None

    def connect(self):
        """Establish connection to PostgreSQL database and create session factory"""
        try:
            database_url = (
                f"postgresql://{self.config.user}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.dbname}"
            )

            logger.info(
                f"Connecting to PostgreSQL at {self.config.host}:{self.config.port}/{self.config.dbname}"
            )

            self.engine = create_engine(
                database_url,
                echo=False,
                pool_size=1,  # Minimum 1 connection in the pool
                max_overflow=0,  # No overflow connections
                pool_timeout=30,  # Wait max 30 seconds for a connection
                pool_recycle=1800,  # Recycle connections every 30 minutes
                poolclass=QueuePool,  # Use QueuePool for connection pooling
            )

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )

            logger.info("Successfully connected to PostgreSQL")

            # Initialize the schema
            self._initialize_schema()

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _initialize_schema(self):
        """Create all tables if they don't exist"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise

    def get_session(self) -> Session:
        """
        Get a new database session.
        The caller is responsible for closing the session.

        Returns:
            SQLAlchemy Session instance
        """
        if self.SessionLocal is None:
            self.connect()
        return self.SessionLocal()

    def close(self):
        """Close the database engine"""
        if self.engine:
            try:
                self.engine.dispose()
                logger.info("Database engine disposed")
            except Exception as e:
                logger.error(f"Error disposing database engine: {e}")

    def is_connected(self) -> bool:
        """Check if the engine is active"""
        return self.engine is not None

    def __enter__(self):
        """Context manager entry - returns a new session"""
        if not self.is_connected():
            self.connect()
        return self.get_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - session is managed by caller"""
        # Session closing is handled by the session context manager
        return False
