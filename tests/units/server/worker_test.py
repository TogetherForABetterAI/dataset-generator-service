"""
Unit tests for src.server.worker.Worker

These tests validate all behaviors of the Worker class:
- Initialization of connections and resources
- Job processing (success and error paths)
- Signal handling
- Cleanup
- Main run loop

All external dependencies (RabbitMQ, Database, ClientManager) are mocked.
Tests run without spawning real processes or connecting to real services.
"""

import pytest
import signal
from unittest.mock import Mock, MagicMock, patch, call
from src.server.worker import Worker
from src.config.config import GlobalConfig, MiddlewareConfig, DatabaseConfig


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_config():
    """Create a mock GlobalConfig with reasonable defaults"""
    config = Mock(spec=GlobalConfig)
    config.worker_pool_size = 3
    config.pod_name = "test-pod"
    config.batch_size = 64
    config.batch_commit_size = 10

    # Mock middleware config
    middleware_config = Mock(spec=MiddlewareConfig)
    middleware_config.host = "localhost"
    middleware_config.port = 5672
    middleware_config.username = "guest"
    middleware_config.password = "guest"
    middleware_config.max_retries = 5
    config.middleware_config = middleware_config

    # Mock database config
    database_config = Mock(spec=DatabaseConfig)
    config.database_config = database_config

    return config


@pytest.fixture
def mock_jobs_queue():
    """Create a mock jobs queue"""
    queue = Mock()
    queue.get = Mock()
    queue.put = Mock()
    return queue


@pytest.fixture
def mock_shutdown_queue():
    """Create a mock shutdown queue"""
    queue = Mock()
    queue.put = Mock()
    return queue


@pytest.fixture
def mock_shared_datasets():
    """Create a mock shared datasets object"""
    return Mock()


@pytest.fixture
def mock_middleware():
    """Create a mock RabbitMQMiddleware"""
    middleware = Mock()
    middleware.connect = Mock(return_value=Mock())
    middleware.create_channel = Mock(return_value=Mock())
    middleware.close = Mock()
    middleware.nack_message = Mock()
    middleware.publish_with_transaction = Mock()
    return middleware


@pytest.fixture
def mock_db_client():
    """Create a mock DatabaseClient"""
    db_client = Mock()
    db_client.close = Mock()
    return db_client


@pytest.fixture
def mock_client_manager():
    """Create a mock ClientManager"""
    # ClientManagerFactory.create returns a dict with handle_client function
    # But worker.py accesses it as client_manager.handle_client()
    # So we create a Mock object that has the handle_client callable
    client_manager = Mock()
    client_manager.handle_client = Mock(
        return_value={"status": "completed", "batches_generated": 5}
    )
    return client_manager


@pytest.fixture
def valid_notification_dict():
    """Create a valid notification dictionary"""
    return {
        "client_id": "client-123",
        "session_id": "session-456",
        "model_type": "ACDC",
        "inputs_format": "image",
        "outputs_format": "label",
    }


@pytest.fixture
def valid_job(valid_notification_dict):
    """Create a valid job dictionary"""
    return {"notification": valid_notification_dict, "delivery_tag": 42}


# ============================================================================
# CONSTRUCTOR TESTS
# ============================================================================


def test_worker_init(mock_config, mock_jobs_queue, mock_shared_datasets, monkeypatch):
    """Test Worker initialization sets up all attributes correctly"""
    # Prevent multiprocessing.Queue creation
    mock_queue_class = Mock(return_value=Mock())
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    worker = Worker(
        worker_id=1,
        config=mock_config,
        jobs_queue=mock_jobs_queue,
        shared_datasets=mock_shared_datasets,
    )

    # Verify attributes
    assert worker.worker_id == 1
    assert worker.config == mock_config
    assert worker.jobs_queue == mock_jobs_queue
    assert worker.shared_datasets == mock_shared_datasets

    # Verify shutdown_queue was created
    mock_queue_class.assert_called_once_with(maxsize=1)

    # Verify resources are None before initialization
    assert worker.middleware is None
    assert worker.db_client is None
    assert worker.client_manager is None
    assert worker.channel is None


# ============================================================================
# _initialize() TESTS
# ============================================================================


def test_initialize_success(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    mock_client_manager,
    monkeypatch,
):
    """Test _initialize() sets up all connections and resources correctly"""
    # Mock external dependencies
    mock_middleware_class = Mock(return_value=mock_middleware)
    mock_db_class = Mock(return_value=mock_db_client)
    mock_factory = Mock(return_value=mock_client_manager)
    mock_queue_class = Mock(return_value=Mock())

    monkeypatch.setattr("src.server.worker.RabbitMQMiddleware", mock_middleware_class)
    monkeypatch.setattr("src.server.worker.DatabaseClient", mock_db_class)
    monkeypatch.setattr("src.server.worker.ClientManagerFactory.create", mock_factory)
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Call _initialize
    worker._initialize()

    # Verify middleware was created with correct config
    mock_middleware_class.assert_called_once_with(mock_config.middleware_config)

    # Verify connection flow
    mock_middleware.connect.assert_called_once()
    connection = mock_middleware.connect.return_value
    mock_middleware.create_channel.assert_called_once_with(connection)

    # Verify database client was created
    mock_db_class.assert_called_once_with(mock_config.database_config)

    # Verify ClientManagerFactory.create was called with correct arguments
    mock_factory.assert_called_once_with(
        batch_size=mock_config.batch_size,
        batch_commit_size=mock_config.batch_commit_size,
        middleware=mock_middleware,
        db_client=mock_db_client,
        channel=mock_middleware.create_channel.return_value,
        shutdown_queue=worker.shutdown_queue,
        shared_datasets=mock_shared_datasets,
    )

    # Verify worker attributes are set
    assert worker.middleware == mock_middleware
    assert worker.db_client == mock_db_client
    assert worker.client_manager == mock_client_manager
    assert worker.channel == mock_middleware.create_channel.return_value


# ============================================================================
# _process_job() TESTS - SUCCESS PATH
# ============================================================================


def test_process_job_success(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    valid_job,
    monkeypatch,
):
    """Test _process_job() successfully processes a valid job"""
    # Mock ConnectNotification
    mock_notification = Mock()
    mock_notification.client_id = "client-123"
    mock_notification.session_id = "session-456"
    mock_from_dict = Mock(return_value=mock_notification)

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and manually set up resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = Mock()

    # Process job
    worker._process_job(valid_job)

    # Verify notification was parsed
    mock_from_dict.assert_called_once_with(valid_job["notification"])

    # Verify handle_client was called with correct arguments
    mock_client_manager.handle_client.assert_called_once_with(
        notification=mock_notification, delivery_tag=valid_job["delivery_tag"]
    )

    # Verify no NACK was called (success path)
    mock_middleware.nack_message.assert_not_called()


# ============================================================================
# _process_job() TESTS - ERROR PATH
# ============================================================================


def test_process_job_handle_client_raises_exception(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    valid_job,
    monkeypatch,
):
    """Test _process_job() handles exceptions from handle_client and NACKs message"""
    # Mock ConnectNotification
    mock_notification = Mock()
    mock_notification.client_id = "client-123"
    mock_notification.session_id = "session-456"
    mock_from_dict = Mock(return_value=mock_notification)

    # Make handle_client raise an exception
    error = RuntimeError("Database connection failed")
    mock_client_manager.handle_client.side_effect = error

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set up resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = Mock()

    # Process job - should not raise
    worker._process_job(valid_job)

    # Verify handle_client was called
    mock_client_manager.handle_client.assert_called_once()

    # Verify message was NACKed with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=worker.channel, delivery_tag=valid_job["delivery_tag"], requeue=False
    )


def test_process_job_nack_also_fails(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    valid_job,
    monkeypatch,
):
    """Test _process_job() handles exceptions when NACK itself fails"""
    # Mock ConnectNotification
    mock_notification = Mock()
    mock_from_dict = Mock(return_value=mock_notification)

    # Make handle_client raise exception
    mock_client_manager.handle_client.side_effect = RuntimeError("First error")

    # Make nack_message also raise exception
    mock_middleware.nack_message.side_effect = Exception("NACK failed")

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set up resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = Mock()

    # Process job - should not raise even though both operations fail
    worker._process_job(valid_job)

    # Verify both were attempted
    mock_client_manager.handle_client.assert_called_once()
    mock_middleware.nack_message.assert_called_once()


def test_process_job_from_dict_raises_exception(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    valid_job,
    monkeypatch,
):
    """Test _process_job() handles exceptions from ConnectNotification.from_dict"""
    # Make from_dict raise exception
    mock_from_dict = Mock(side_effect=ValueError("Invalid notification format"))

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set up resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = Mock()

    # Process job - should not raise
    worker._process_job(valid_job)

    # Verify from_dict was called
    mock_from_dict.assert_called_once()

    # Verify handle_client was NOT called
    mock_client_manager.handle_client.assert_not_called()

    # Verify message was NACKed
    mock_middleware.nack_message.assert_called_once_with(
        channel=worker.channel, delivery_tag=valid_job["delivery_tag"], requeue=False
    )


# ============================================================================
# _signal_handler() TESTS
# ============================================================================


def test_signal_handler_puts_shutdown_signal(
    mock_config, mock_jobs_queue, mock_shared_datasets, monkeypatch
):
    """Test _signal_handler() puts None into shutdown_queue"""
    mock_shutdown_queue = Mock()
    mock_queue_class = Mock(return_value=mock_shutdown_queue)

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Call signal handler
    worker._signal_handler(signal.SIGTERM, None)

    # Verify shutdown signal was sent
    mock_shutdown_queue.put.assert_called_once_with(None, block=False)


def test_signal_handler_handles_queue_full(
    mock_config, mock_jobs_queue, mock_shared_datasets, monkeypatch
):
    """Test _signal_handler() doesn't raise when queue is full"""
    mock_shutdown_queue = Mock()
    mock_shutdown_queue.put.side_effect = Exception("Queue full")
    mock_queue_class = Mock(return_value=mock_shutdown_queue)

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Call signal handler - should not raise
    worker._signal_handler(signal.SIGINT, None)

    # Verify put was attempted
    mock_shutdown_queue.put.assert_called_once()


# ============================================================================
# _cleanup() TESTS
# ============================================================================


def test_cleanup_closes_middleware_and_db(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    monkeypatch,
):
    """Test _cleanup() closes middleware and database connections"""
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.db_client = mock_db_client

    # Call cleanup
    worker._cleanup()

    # Verify both were closed
    mock_middleware.close.assert_called_once()
    mock_db_client.close.assert_called_once()


def test_cleanup_handles_middleware_close_error(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    monkeypatch,
):
    """Test _cleanup() handles exceptions when closing middleware"""
    mock_middleware.close.side_effect = Exception("Connection already closed")

    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.db_client = mock_db_client

    # Call cleanup - should not raise
    worker._cleanup()

    # Verify middleware.close was attempted
    mock_middleware.close.assert_called_once()

    # Verify db_client.close was still called
    mock_db_client.close.assert_called_once()


def test_cleanup_handles_db_close_error(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    monkeypatch,
):
    """Test _cleanup() handles exceptions when closing database"""
    mock_db_client.close.side_effect = Exception("Connection error")

    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.db_client = mock_db_client

    # Call cleanup - should not raise
    worker._cleanup()

    # Verify both close attempts were made
    mock_middleware.close.assert_called_once()
    mock_db_client.close.assert_called_once()


def test_cleanup_with_no_resources(
    mock_config, mock_jobs_queue, mock_shared_datasets, monkeypatch
):
    """Test _cleanup() handles case where resources were never initialized"""
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker without initializing resources
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Call cleanup - should not raise
    worker._cleanup()

    # No assertions needed - just verify it doesn't crash


# ============================================================================
# run() TESTS - WITHOUT SPAWNING REAL PROCESSES
# ============================================================================


def test_run_processes_jobs_until_shutdown_signal(
    mock_config, mock_jobs_queue, mock_shared_datasets, valid_job, monkeypatch
):
    """Test run() processes jobs and stops on None (shutdown signal)"""
    # Mock all dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Mock internal methods
    worker._initialize = Mock()
    worker._process_job = Mock()
    worker._cleanup = Mock()

    # Mock jobs_queue.get to return: job1, job2, None (shutdown)
    mock_jobs_queue.get.side_effect = [valid_job, valid_job.copy(), None]

    # Run the worker (without actually spawning a process)
    worker.run()

    # Verify signal handlers were registered
    assert mock_signal.call_count == 2
    signal_calls = mock_signal.call_args_list
    assert signal_calls[0][0][0] == signal.SIGTERM
    assert signal_calls[1][0][0] == signal.SIGINT

    # Verify _initialize was called
    worker._initialize.assert_called_once()

    # Verify _process_job was called twice (for the two jobs)
    assert worker._process_job.call_count == 2
    worker._process_job.assert_any_call(valid_job)

    # Verify _cleanup was called
    worker._cleanup.assert_called_once()


def test_run_handles_exception_in_job_processing(
    mock_config, mock_jobs_queue, mock_shared_datasets, valid_job, monkeypatch
):
    """Test run() continues processing after job error"""
    # Mock dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Mock internal methods
    worker._initialize = Mock()
    worker._cleanup = Mock()

    # Make _process_job fail on first call, succeed on second
    worker._process_job = Mock(
        side_effect=[
            RuntimeError("Processing failed"),
            None,  # Second call succeeds
        ]
    )

    # Mock jobs_queue.get to return: job1 (will fail), job2 (will succeed), None
    mock_jobs_queue.get.side_effect = [valid_job, valid_job.copy(), None]

    # Run the worker
    worker.run()

    # Verify both jobs were attempted
    assert worker._process_job.call_count == 2

    # Verify cleanup was still called
    worker._cleanup.assert_called_once()


def test_run_handles_fatal_error_during_initialization(
    mock_config, mock_jobs_queue, mock_shared_datasets, monkeypatch
):
    """Test run() handles fatal error during initialization"""
    # Mock dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Make _initialize raise fatal error
    worker._initialize = Mock(side_effect=RuntimeError("Cannot connect to RabbitMQ"))
    worker._process_job = Mock()
    worker._cleanup = Mock()

    # Run the worker - should not crash
    worker.run()

    # Verify _initialize was attempted
    worker._initialize.assert_called_once()

    # Verify _process_job was NOT called
    worker._process_job.assert_not_called()

    # Verify _cleanup was still called
    worker._cleanup.assert_called_once()


def test_run_empty_queue_waits_for_jobs(
    mock_config, mock_jobs_queue, mock_shared_datasets, valid_job, monkeypatch
):
    """Test run() blocks waiting for jobs when queue is empty"""
    # Mock dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Mock internal methods
    worker._initialize = Mock()
    worker._process_job = Mock()
    worker._cleanup = Mock()

    # Mock jobs_queue.get to immediately return None (shutdown)
    mock_jobs_queue.get.side_effect = [None]

    # Run the worker
    worker.run()

    # Verify get was called with block=True
    mock_jobs_queue.get.assert_called_once_with(block=True)

    # Verify _process_job was NOT called (no real jobs)
    worker._process_job.assert_not_called()

    # Verify cleanup was called
    worker._cleanup.assert_called_once()


# ============================================================================
# INTEGRATION-STYLE TESTS (still unit tests, no real processes)
# ============================================================================


def test_full_worker_lifecycle(
    mock_config,
    mock_jobs_queue,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    mock_client_manager,
    valid_job,
    monkeypatch,
):
    """Test complete worker lifecycle: init -> process -> shutdown -> cleanup"""
    # Mock all external dependencies
    mock_middleware_class = Mock(return_value=mock_middleware)
    mock_db_class = Mock(return_value=mock_db_client)
    mock_factory = Mock(return_value=mock_client_manager)
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()
    mock_notification = Mock()
    mock_notification.client_id = "client-123"
    mock_notification.session_id = "session-456"
    mock_from_dict = Mock(return_value=mock_notification)

    monkeypatch.setattr("src.server.worker.RabbitMQMiddleware", mock_middleware_class)
    monkeypatch.setattr("src.server.worker.DatabaseClient", mock_db_class)
    monkeypatch.setattr("src.server.worker.ClientManagerFactory.create", mock_factory)
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)
    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Mock jobs_queue to return one job then shutdown
    mock_jobs_queue.get.side_effect = [valid_job, None]

    # Run worker
    worker.run()

    # Verify initialization happened
    mock_middleware_class.assert_called_once()
    mock_db_class.assert_called_once()
    mock_factory.assert_called_once()

    # Verify job was processed
    mock_from_dict.assert_called_once_with(valid_job["notification"])
    mock_client_manager.handle_client.assert_called_once()

    # Verify cleanup happened
    mock_middleware.close.assert_called_once()
    mock_db_client.close.assert_called_once()


def test_worker_processes_multiple_jobs_before_shutdown(
    mock_config, mock_jobs_queue, mock_shared_datasets, valid_job, monkeypatch
):
    """Test worker processes multiple jobs in sequence"""
    # Mock dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_jobs_queue, mock_shared_datasets)

    # Mock internal methods
    worker._initialize = Mock()
    worker._process_job = Mock()
    worker._cleanup = Mock()

    # Create multiple jobs
    job1 = valid_job.copy()
    job2 = valid_job.copy()
    job2["notification"]["session_id"] = "session-789"
    job3 = valid_job.copy()
    job3["notification"]["session_id"] = "session-000"

    # Mock jobs_queue to return multiple jobs then shutdown
    mock_jobs_queue.get.side_effect = [job1, job2, job3, None]

    # Run worker
    worker.run()

    # Verify all three jobs were processed
    assert worker._process_job.call_count == 3
    worker._process_job.assert_any_call(job1)
    worker._process_job.assert_any_call(job2)
    worker._process_job.assert_any_call(job3)
