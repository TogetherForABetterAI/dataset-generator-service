import pytest
import uuid
import time
from tests.e2e.utils import (
    publish_connect_message,
    create_ephemeral_dispatcher_queue,
    wait_for_dispatcher_message,
    get_batch_count,
    cleanup_session_batches,
)


def test_e2e_full_dataset_generation(rabbitmq_channel, db_session):
    """
    End-to-end test of the complete dataset generation flow.

    Flow:
    1. Publish ConnectNotification to new_connections_exchange
    2. Listener picks up message and dispatches to Worker
    3. Worker processes job via BatchHandler and BatchRepository
    4. Batches are saved to PostgreSQL
    5. ClientManager publishes NotifyDispatcher to dispatcher_exchange
    6. Test consumes dispatcher message and verifies batch count in DB
    """
    session_id = str(uuid.uuid4())
    client_id = "test-client-001"
    model_type = "mnist"

    print(f"\n[E2E] Starting test with session_id={session_id}")

    dispatcher_queue = create_ephemeral_dispatcher_queue(rabbitmq_channel)
    print(f"[E2E] Created ephemeral dispatcher queue: {dispatcher_queue}")

    publish_connect_message(
        rabbitmq_channel,
        client_id=client_id,
        session_id=session_id,
        model_type=model_type,
    )
    print(f"[E2E] Published ConnectNotification for {model_type}")

    print("[E2E] Waiting for dispatcher notification...")
    try:
        dispatcher_msg = wait_for_dispatcher_message(
            rabbitmq_channel, dispatcher_queue, timeout=60
        )
    except TimeoutError as e:
        pytest.fail(f"Timeout waiting for dispatcher message: {e}")

    print(f"[E2E] Received dispatcher message: {dispatcher_msg}")

    assert "client_id" in dispatcher_msg, "Missing client_id in dispatcher message"
    assert "session_id" in dispatcher_msg, "Missing session_id in dispatcher message"
    assert (
        "total_batches_generated" in dispatcher_msg
    ), "Missing total_batches_generated in dispatcher message"

    assert (
        dispatcher_msg["client_id"] == client_id
    ), f"client_id mismatch: expected {client_id}, got {dispatcher_msg['client_id']}"
    assert (
        dispatcher_msg["session_id"] == session_id
    ), f"session_id mismatch: expected {session_id}, got {dispatcher_msg['session_id']}"

    expected_batches = dispatcher_msg["total_batches_generated"]
    assert (
        expected_batches > 0
    ), f"Expected positive batch count, got {expected_batches}"

    print(f"[E2E] Dispatcher reports {expected_batches} batches generated")

    print("[E2E] Verifying batches in PostgreSQL...")
    time.sleep(1)

    actual_batches = get_batch_count(db_session, session_id)
    print(f"[E2E] Found {actual_batches} batches in database")

    assert actual_batches == expected_batches, (
        f"Batch count mismatch: dispatcher reported {expected_batches}, "
        f"but database contains {actual_batches}"
    )

    print(
        f"[E2E] ✓ Test passed: {actual_batches} batches successfully generated and persisted"
    )

    cleanup_session_batches(db_session, session_id)
    print(f"[E2E] Cleaned up test data for session {session_id}")
