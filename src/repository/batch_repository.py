import json
import logging
from typing import List, Tuple
import numpy as np
from src.db.client import DatabaseClient

logger = logging.getLogger(__name__)


class BatchRepository:
    """
    Repository pattern for batch data persistence.
    Encapsulates all SQL logic for the batches table.
    """

    def __init__(self, db_client: DatabaseClient):
        self.db_client = db_client

    def insert_batch(
        self,
        session_id: str,
        batch_index: int,
        data_payload: np.ndarray,
        labels: List[int],
    ) -> bool:
        """
        Insert a single batch into the database.

        Args:
            session_id: The session identifier
            batch_index: The index of the batch
            data_payload: The numpy array containing the batch data
            labels: List of labels for the batch

        Returns:
            True if insertion was successful, False otherwise
        """
        try:
            # Serialize numpy array to bytes
            data_bytes = data_payload.tobytes()

            # Convert labels to JSON
            labels_json = json.dumps(labels)

            insert_sql = """
                INSERT INTO batches (session_id, batch_index, data_payload, labels, isEnqueued, created_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (session_id, batch_index) DO NOTHING;
            """

            with self.db_client.get_cursor() as cursor:
                cursor.execute(
                    insert_sql,
                    (session_id, batch_index, data_bytes, labels_json, False),
                )
                rows_affected = cursor.rowcount

            logger.debug(
                f"Inserted batch: session_id={session_id}, batch_index={batch_index}, rows_affected={rows_affected}"
            )
            return rows_affected > 0

        except Exception as e:
            logger.error(
                f"Failed to insert batch (session_id={session_id}, batch_index={batch_index}): {e}"
            )
            raise

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
        inserted_count = 0

        try:
            insert_sql = """
                INSERT INTO batches (session_id, batch_index, data_payload, labels, isEnqueued, created_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (session_id, batch_index) DO NOTHING;
            """

            with self.db_client.get_cursor() as cursor:
                for batch_index, data_payload, labels in batches:
                    # Serialize data
                    data_bytes = data_payload.tobytes()
                    labels_json = json.dumps(labels)

                    cursor.execute(
                        insert_sql,
                        (session_id, batch_index, data_bytes, labels_json, False),
                    )
                    inserted_count += cursor.rowcount

            logger.info(
                f"Bulk insert completed: session_id={session_id}, inserted={inserted_count}/{len(batches)} batches"
            )
            return inserted_count

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
            select_sql = """
                SELECT data_payload, labels
                FROM batches
                WHERE session_id = %s AND batch_index = %s;
            """

            with self.db_client.get_cursor() as cursor:
                cursor.execute(select_sql, (session_id, batch_index))
                result = cursor.fetchone()

                if result is None:
                    raise ValueError(
                        f"Batch not found: session_id={session_id}, batch_index={batch_index}"
                    )

                data_payload, labels_json = result
                labels = json.loads(labels_json)
                return data_payload, labels

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
            count_sql = """
                SELECT COUNT(*)
                FROM batches
                WHERE session_id = %s;
            """

            with self.db_client.get_cursor() as cursor:
                cursor.execute(count_sql, (session_id,))
                result = cursor.fetchone()
                return result[0] if result else 0

        except Exception as e:
            logger.error(f"Failed to count batches for session_id={session_id}: {e}")
            raise

    def mark_batch_enqueued(self, session_id: str, batch_index: int) -> bool:
        """
        Mark a batch as enqueued (isEnqueued = True).

        Args:
            session_id: The session identifier
            batch_index: The index of the batch

        Returns:
            True if update was successful
        """
        try:
            update_sql = """
                UPDATE batches
                SET isEnqueued = TRUE
                WHERE session_id = %s AND batch_index = %s;
            """

            with self.db_client.get_cursor() as cursor:
                cursor.execute(update_sql, (session_id, batch_index))
                rows_affected = cursor.rowcount

            logger.debug(
                f"Marked batch as enqueued: session_id={session_id}, batch_index={batch_index}"
            )
            return rows_affected > 0

        except Exception as e:
            logger.error(
                f"Failed to mark batch as enqueued (session_id={session_id}, batch_index={batch_index}): {e}"
            )
            raise
