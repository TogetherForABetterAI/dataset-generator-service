import json
import logging
from typing import List, Tuple
import numpy as np
from src.db.client import DatabaseClient
from src.schemas.batch import Batch

logger = logging.getLogger(__name__)


class BatchRepository:
    """
    Repository pattern for batch data persistence using SQLAlchemy.
    Encapsulates all SQL logic for the batches table.
    """

    def __init__(self, db_client: DatabaseClient):
        self.db_client = db_client

    def insert_batches_bulk(
        self, session_id: str, batches: List[Tuple[int, np.ndarray, List[int]]]
    ) -> int:
        """
        Insert multiple batches in a single transaction for better performance.

        Args:
            session_id: The session identifier
            batches: List of tuples (batch_index, data_payload, labels)

        Returns:
            Number of batches successfully inserted
        """
        if not batches:
            return 0

        try:
            with self.db_client.get_session() as db_session:
                batch_objects = []
                for batch_index, data_payload, labels in batches:
                    # Serialize numpy array to bytes
                    data_bytes = data_payload.tobytes()

                    batch_obj = Batch(
                        session_id=session_id,
                        batch_index=batch_index,
                        data_payload=data_bytes,
                        labels=labels,
                        isEnqueued=False,
                    )
                    batch_objects.append(batch_obj)

                # Bulk insert
                db_session.add_all(batch_objects)
                db_session.commit()

            logger.info(
                f"Bulk insert completed: session_id={session_id}, inserted={len(batch_objects)} batches"
            )
            return len(batch_objects)

        except Exception as e:
            logger.error(
                f"Failed to bulk insert batches for session_id={session_id}: {e}"
            )
            raise

    def get_batch(self, session_id: str, batch_index: int) -> Tuple[bytes, List[int]]:
        """
        Retrieve a single batch from the database.

        Args:
            session_id: The session identifier
            batch_index: The index of the batch

        Returns:
            Tuple of (data_payload_bytes, labels)
        """
        try:
            with self.db_client.get_session() as db_session:
                batch = (
                    db_session.query(Batch)
                    .filter(
                        Batch.session_id == session_id, Batch.batch_index == batch_index
                    )
                    .first()
                )

                if batch is None:
                    raise ValueError(
                        f"Batch not found: session_id={session_id}, batch_index={batch_index}"
                    )

                return batch.data_payload, batch.labels

        except Exception as e:
            logger.error(
                f"Failed to retrieve batch (session_id={session_id}, batch_index={batch_index}): {e}"
            )
            raise

    def get_batch_count(self, session_id: str) -> int:
        """
        Get the total number of batches for a session.

        Args:
            session_id: The session identifier

        Returns:
            Number of batches
        """
        try:
            with self.db_client.get_session() as db_session:
                count = (
                    db_session.query(Batch)
                    .filter(Batch.session_id == session_id)
                    .count()
                )
                return count

        except Exception as e:
            logger.error(f"Failed to count batches for session_id={session_id}: {e}")
            raise
