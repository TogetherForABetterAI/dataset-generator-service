import json
import time
import pika
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text


def publish_connect_message(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    user_id: str,
    session_id: str,
    model_type: str,
) -> None:
    """
    Publish a ConnectNotification message to the new_connections_exchange.

    Args:
        channel: RabbitMQ channel
        user_id: Client identifier
        session_id: Session identifier
        model_type: Type of model (mnist, acdc)
    """
    message = {
        "user_id": user_id,
        "session_id": session_id,
        "model_type": model_type,
    }

    channel.basic_publish(
        exchange="new_connections_exchange",
        routing_key="",
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2, content_type="application/json"
        ),
    )


def create_ephemeral_dispatcher_queue(
    channel: pika.adapters.blocking_connection.BlockingChannel,
) -> str:
    """
    Create an exclusive, auto-delete queue bound to dispatcher_exchange.

    Args:
        channel: RabbitMQ channel

    Returns:
        The name of the created queue
    """
    result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
    queue_name = result.method.queue

    channel.queue_bind(queue=queue_name, exchange="dispatcher_exchange")

    return queue_name


def wait_for_dispatcher_message(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Poll for a message from the dispatcher queue.

    Args:
        channel: RabbitMQ channel
        queue_name: Queue to consume from
        timeout: Maximum time to wait in seconds

    Returns:
        Parsed message dictionary

    Raises:
        TimeoutError: If no message is received within timeout
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        method_frame, properties, body = channel.basic_get(
            queue=queue_name, auto_ack=True
        )

        if method_frame:
            return json.loads(body)

        time.sleep(0.3)

    raise TimeoutError(
        f"No message received from dispatcher queue within {timeout} seconds"
    )


def get_batch_count(db_session: Session, session_id: str) -> int:
    """
    Get the count of batches for a specific session.

    Args:
        db_session: SQLAlchemy session
        session_id: Session identifier

    Returns:
        Number of batches in the database
    """
    result = db_session.execute(
        text("SELECT COUNT(*) FROM batches WHERE session_id = :session_id"),
        {"session_id": session_id},
    )
    return result.scalar()


def wait_for_batches(
    db_session: Session, session_id: str, expected: int, timeout: int = 30
) -> int:
    """
    Poll until the expected number of batches appears in the database.

    Args:
        db_session: SQLAlchemy session
        session_id: Session identifier
        expected: Expected number of batches
        timeout: Maximum time to wait in seconds

    Returns:
        Actual number of batches found

    Raises:
        TimeoutError: If expected batches not found within timeout
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        count = get_batch_count(db_session, session_id)

        if count == expected:
            return count

        if count > expected:
            return count

        time.sleep(0.3)

    count = get_batch_count(db_session, session_id)
    raise TimeoutError(
        f"Expected {expected} batches, but found {count} after {timeout} seconds"
    )


def cleanup_session_batches(db_session: Session, session_id: str) -> None:
    """
    Delete all batches for a specific session.

    Args:
        db_session: SQLAlchemy session
        session_id: Session identifier
    """
    db_session.execute(
        text("DELETE FROM batches WHERE session_id = :session_id"),
        {"session_id": session_id},
    )
    db_session.commit()
