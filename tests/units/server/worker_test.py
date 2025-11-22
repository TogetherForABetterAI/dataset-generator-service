"""
Unit tests for src.server.worker.Worker

These tests validate all behaviors of the Worker class in the new architecture:
- Initialization of connections and resources
- Direct RabbitMQ consumption (no jobs_queue)
- Message callback processing (success and error paths)
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
    """Create a mock Middleware"""
    middleware = Mock()
    middleware.connect = Mock(return_value=Mock())
    middleware.create_channel = Mock(return_value=Mock())
    middleware.set_qos = Mock()
    middleware.close = Mock()
    middleware.nack_message = Mock()
    middleware.ack_message = Mock()
    middleware.publish_with_transaction = Mock()
    middleware.start_consuming = Mock()
    middleware.stop_consuming = Mock()
    return middleware


@pytest.fixture
def mock_db_client():
    """Create a mock DatabaseClient"""
    db_client = Mock()
    db_client.close = Mock()
    return db_client


@pytest.fixture
def mock_client_manager():
    """Create a mock ClientManager (it's a dictionary with handle_client function)"""
    handle_client_mock = Mock(
        return_value={"status": "completed", "batches_generated": 5}
    )
    return {"handle_client": handle_client_mock}


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
def mock_channel():
    """Create a mock RabbitMQ channel"""
    return Mock()


@pytest.fixture
def mock_method():
    """Create a mock RabbitMQ method frame"""
    method = Mock()
    method.delivery_tag = 42
    return method


@pytest.fixture
def mock_properties():
    """Create a mock RabbitMQ properties"""
    return Mock()


# ============================================================================
# CONSTRUCTOR TESTS
# ============================================================================


def test_worker_init(mock_config, mock_shared_datasets, monkeypatch):
    """Test Worker initialization sets up all attributes correctly"""
    # Prevent multiprocessing.Queue creation
    mock_queue_class = Mock(return_value=Mock())
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    worker = Worker(
        worker_id=1,
        config=mock_config,
        shared_datasets=mock_shared_datasets,
    )

    # Verify attributes
    assert worker.worker_id == 1
    assert worker.config == mock_config
    assert worker.shared_datasets == mock_shared_datasets

    # Verify shutdown_queue was created
    mock_queue_class.assert_called_once_with(maxsize=1)

    # Verify resources are None before initialization
    assert worker.middleware is None
    assert worker.db_client is None
    assert worker.client_manager is None
    assert worker.channel is None
    assert worker.consumer_tag is None


# ============================================================================
# _initialize() TESTS
# ============================================================================


def test_initialize_success(
    mock_config,
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

    monkeypatch.setattr("src.server.worker.Middleware", mock_middleware_class)
    monkeypatch.setattr("src.server.worker.DatabaseClient", mock_db_class)
    monkeypatch.setattr("src.server.worker.ClientManagerFactory.create", mock_factory)
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Call _initialize
    worker._initialize()

    # Verify middleware was created with correct config
    mock_middleware_class.assert_called_once_with(mock_config.middleware_config)

    # Verify connection flow
    mock_middleware.connect.assert_called_once()
    connection = mock_middleware.connect.return_value
    mock_middleware.create_channel.assert_called_once_with(connection)

    # Verify QoS was configured with prefetch_count=1
    mock_middleware.set_qos.assert_called_once_with(
        channel=mock_middleware.create_channel.return_value, prefetch_count=1
    )

    # Verify database client was created
    mock_db_class.assert_called_once_with(mock_config.database_config)

    # Verify ClientManagerFactory.create was called with correct arguments
    mock_factory.assert_called_once()
    call_kwargs = mock_factory.call_args[1]

    assert call_kwargs["batch_size"] == mock_config.batch_size
    assert call_kwargs["middleware"] == mock_middleware
    assert call_kwargs["channel"] == mock_middleware.create_channel.return_value

    # Verify batch_handler was passed and is a BatchHandler instance
    from src.server.batch_handler import BatchHandler

    assert isinstance(call_kwargs["batch_handler"], BatchHandler)

    # Verify worker attributes are set
    assert worker.middleware == mock_middleware
    assert worker.db_client == mock_db_client
    assert worker.client_manager == mock_client_manager
    assert worker.channel == mock_middleware.create_channel.return_value


# ============================================================================
# _start_consuming() TESTS
# ============================================================================


def test_start_consuming_registers_callback_and_starts(
    mock_config, mock_shared_datasets, mock_middleware, monkeypatch
):
    """Test _start_consuming() registers callback and starts consuming"""
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.channel = Mock()

    # Mock consume to return a consumer tag
    mock_middleware.consume.return_value = "consumer-tag-123"

    # Call _start_consuming
    worker._start_consuming()

    # Verify consume was called with callback
    mock_middleware.consume.assert_called_once()
    call_args = mock_middleware.consume.call_args
    assert call_args[1]["channel"] == worker.channel
    assert "queue" in call_args[1]
    assert "consumer_tag" in call_args[1]
    assert callable(call_args[1]["callback"])
    assert call_args[1]["auto_ack"] is False

    # Verify consumer_tag was stored
    assert worker.consumer_tag == "consumer-tag-123"

    # Verify start_consuming was called
    mock_middleware.start_consuming.assert_called_once_with(worker.channel)


def test_start_consuming_callback_success_path(
    mock_config,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    mock_channel,
    mock_method,
    mock_properties,
    valid_notification_dict,
    monkeypatch,
):
    """Test that the callback created by _start_consuming processes messages correctly"""
    # Mock ConnectNotification
    mock_notification = Mock()
    mock_notification.client_id = "client-123"
    mock_notification.session_id = "session-456"
    mock_notification.validate.return_value = True
    mock_from_dict = Mock(return_value=mock_notification)

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and manually set up resources
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = mock_channel

    # Mock consume to capture the callback
    captured_callback = None

    def capture_callback(*args, **kwargs):
        nonlocal captured_callback
        captured_callback = kwargs["callback"]
        return "consumer-tag-123"

    mock_middleware.consume.side_effect = capture_callback

    # Call _start_consuming to register callback
    worker._start_consuming()

    # Now call the captured callback with a message
    import json

    body = json.dumps(valid_notification_dict).encode("utf-8")
    captured_callback(mock_channel, mock_method, mock_properties, body)

    # Verify notification was parsed
    mock_from_dict.assert_called_once_with(valid_notification_dict)

    # Verify handle_client was called with correct arguments
    mock_client_manager["handle_client"].assert_called_once_with(
        notification=mock_notification, delivery_tag=mock_method.delivery_tag
    )

    # Verify no NACK was called (success path)
    mock_middleware.nack_message.assert_not_called()


def test_start_consuming_callback_handles_exception(
    mock_config,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    mock_channel,
    mock_method,
    mock_properties,
    valid_notification_dict,
    monkeypatch,
):
    """Test that the callback handles exceptions from handle_client and NACKs message"""
    # Mock ConnectNotification
    mock_notification = Mock()
    mock_notification.client_id = "client-123"
    mock_notification.session_id = "session-456"
    mock_notification.validate.return_value = True
    mock_from_dict = Mock(return_value=mock_notification)

    # Make handle_client raise an exception
    error = RuntimeError("Database connection failed")
    mock_client_manager["handle_client"].side_effect = error

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set up resources
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = mock_channel

    # Mock consume to capture the callback
    captured_callback = None

    def capture_callback(*args, **kwargs):
        nonlocal captured_callback
        captured_callback = kwargs["callback"]
        return "consumer-tag-123"

    mock_middleware.consume.side_effect = capture_callback

    # Call _start_consuming to register callback
    worker._start_consuming()

    # Now call the captured callback with a message
    import json

    body = json.dumps(valid_notification_dict).encode("utf-8")
    captured_callback(mock_channel, mock_method, mock_properties, body)

    # Verify handle_client was called
    mock_client_manager["handle_client"].assert_called_once()

    # Verify message was NACKed with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=mock_channel, delivery_tag=mock_method.delivery_tag, requeue=False
    )


def test_start_consuming_callback_invalid_notification(
    mock_config,
    mock_shared_datasets,
    mock_middleware,
    mock_client_manager,
    mock_channel,
    mock_method,
    mock_properties,
    valid_notification_dict,
    monkeypatch,
):
    """Test that the callback handles invalid notifications and NACKs"""
    # Make from_dict raise exception
    mock_from_dict = Mock(side_effect=ValueError("Invalid notification format"))

    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker and set up resources
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.client_manager = mock_client_manager
    worker.channel = mock_channel

    # Mock consume to capture the callback
    captured_callback = None

    def capture_callback(*args, **kwargs):
        nonlocal captured_callback
        captured_callback = kwargs["callback"]
        return "consumer-tag-123"

    mock_middleware.consume.side_effect = capture_callback

    # Call _start_consuming to register callback
    worker._start_consuming()

    # Now call the captured callback with a message
    import json

    body = json.dumps(valid_notification_dict).encode("utf-8")
    captured_callback(mock_channel, mock_method, mock_properties, body)

    # Verify from_dict was called
    mock_from_dict.assert_called_once()

    # Verify handle_client was NOT called
    # Note: mock_client_manager is a dict, not an object
    assert mock_client_manager["handle_client"].call_count == 0

    # Verify message was NACKed
    mock_middleware.nack_message.assert_called_once_with(
        channel=mock_channel, delivery_tag=mock_method.delivery_tag, requeue=False
    )


# ============================================================================
# _signal_handler() TESTS
# ============================================================================


def test_signal_handler_puts_shutdown_signal(
    mock_config, mock_shared_datasets, monkeypatch
):
    """Test _signal_handler() puts None into shutdown_queue"""
    mock_shutdown_queue = Mock()
    mock_queue_class = Mock(return_value=mock_shutdown_queue)

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Call signal handler
    worker._signal_handler(signal.SIGTERM, None)

    # Verify shutdown signal was sent
    mock_shutdown_queue.put.assert_called_once_with(None, block=False)


def test_signal_handler_handles_queue_full(
    mock_config, mock_shared_datasets, monkeypatch
):
    """Test _signal_handler() doesn't raise when queue is full"""
    mock_shutdown_queue = Mock()
    mock_shutdown_queue.put.side_effect = Exception("Queue full")
    mock_queue_class = Mock(return_value=mock_shutdown_queue)

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Call signal handler - should not raise
    worker._signal_handler(signal.SIGINT, None)

    # Verify put was attempted
    mock_shutdown_queue.put.assert_called_once()


# ============================================================================
# _cleanup() TESTS
# ============================================================================


def test_cleanup_closes_middleware_and_db(
    mock_config,
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
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.db_client = mock_db_client

    # Call cleanup
    worker._cleanup()

    # Verify both connections were closed
    mock_middleware.close.assert_called_once()
    mock_db_client.close.assert_called_once()


def test_cleanup_handles_middleware_close_error(
    mock_config,
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
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.db_client = mock_db_client
    worker.consumer_tag = "test-consumer"
    worker.channel = Mock()

    # Call cleanup - should not raise
    worker._cleanup()

    # Verify middleware.close was attempted
    mock_middleware.close.assert_called_once()

    # Verify db_client.close was still called
    mock_db_client.close.assert_called_once()


def test_cleanup_handles_db_close_error(
    mock_config,
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
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker.middleware = mock_middleware
    worker.db_client = mock_db_client
    worker.consumer_tag = "test-consumer"
    worker.channel = Mock()

    # Call cleanup - should not raise
    worker._cleanup()

    # Verify both close attempts were made
    mock_middleware.close.assert_called_once()
    mock_db_client.close.assert_called_once()


def test_cleanup_with_no_resources(mock_config, mock_shared_datasets, monkeypatch):
    """Test _cleanup() handles case where resources were never initialized"""
    monkeypatch.setattr(
        "src.server.worker.multiprocessing.Queue", Mock(return_value=Mock())
    )

    # Create worker without initializing resources
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Call cleanup - should not raise
    worker._cleanup()

    # No assertions needed - just verify it doesn't crash


# ============================================================================
# run() TESTS - WITHOUT SPAWNING REAL PROCESSES
# ============================================================================


def test_run_initializes_and_starts_consuming(
    mock_config, mock_shared_datasets, mock_middleware, monkeypatch
):
    """Test run() initializes worker and starts consuming from RabbitMQ"""
    # Mock all dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Mock internal methods
    worker._initialize = Mock()
    worker._start_consuming = Mock()
    worker._cleanup = Mock()

    # Run the worker (without actually blocking)
    worker.run()

    # Verify signal handlers were registered
    assert mock_signal.call_count == 2
    signal_calls = mock_signal.call_args_list
    assert signal_calls[0][0][0] == signal.SIGTERM
    assert signal_calls[1][0][0] == signal.SIGINT

    # Verify _initialize was called
    worker._initialize.assert_called_once()

    # Verify _start_consuming was called
    worker._start_consuming.assert_called_once()

    # Verify _cleanup was called
    worker._cleanup.assert_called_once()


def test_run_handles_fatal_error_during_initialization(
    mock_config, mock_shared_datasets, monkeypatch
):
    """Test run() handles fatal error during initialization"""
    # Mock dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Make _initialize raise fatal error
    worker._initialize = Mock(side_effect=RuntimeError("Cannot connect to RabbitMQ"))
    worker._start_consuming = Mock()
    worker._cleanup = Mock()

    # Run the worker - should not crash
    worker.run()

    # Verify _initialize was attempted
    worker._initialize.assert_called_once()

    # Verify _start_consuming was NOT called
    worker._start_consuming.assert_not_called()

    # Verify _cleanup was still called
    worker._cleanup.assert_called_once()


def test_run_handles_exception_during_consuming(
    mock_config, mock_shared_datasets, monkeypatch
):
    """Test run() handles exception during message consumption"""
    # Mock dependencies
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Mock internal methods
    worker._initialize = Mock()
    worker._start_consuming = Mock(side_effect=RuntimeError("Connection lost"))
    worker._cleanup = Mock()

    # Run the worker - should not crash
    worker.run()

    # Verify initialization succeeded
    worker._initialize.assert_called_once()

    # Verify _start_consuming was called
    worker._start_consuming.assert_called_once()

    # Verify cleanup was still called
    worker._cleanup.assert_called_once()


# ============================================================================
# INTEGRATION-STYLE TESTS (still unit tests, no real processes)
# ============================================================================


def test_full_worker_lifecycle(
    mock_config,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    mock_client_manager,
    monkeypatch,
):
    """Test complete worker lifecycle: init -> consume -> cleanup"""
    # Mock all external dependencies
    mock_middleware_class = Mock(return_value=mock_middleware)
    mock_db_class = Mock(return_value=mock_db_client)
    mock_factory = Mock(return_value=mock_client_manager)
    mock_queue_class = Mock(return_value=Mock())
    mock_signal = Mock()

    monkeypatch.setattr("src.server.worker.Middleware", mock_middleware_class)
    monkeypatch.setattr("src.server.worker.DatabaseClient", mock_db_class)
    monkeypatch.setattr("src.server.worker.ClientManagerFactory.create", mock_factory)
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr("src.server.worker.signal.signal", mock_signal)

    # Create worker
    worker = Worker(1, mock_config, mock_shared_datasets)

    # Mock _start_consuming to not block
    worker._start_consuming = Mock()

    # Run worker
    worker.run()

    # Verify initialization happened
    mock_middleware_class.assert_called_once()
    mock_db_class.assert_called_once()
    mock_factory.assert_called_once()

    # Verify consuming was started
    worker._start_consuming.assert_called_once()

    # Verify cleanup happened
    mock_middleware.close.assert_called_once()
    mock_db_client.close.assert_called_once()


def test_callback_integration(
    mock_config,
    mock_shared_datasets,
    mock_middleware,
    mock_db_client,
    mock_client_manager,
    mock_channel,
    mock_method,
    mock_properties,
    valid_notification_dict,
    monkeypatch,
):
    """Test callback integration with all mocked dependencies"""
    # Mock all external dependencies
    mock_middleware_class = Mock(return_value=mock_middleware)
    mock_db_class = Mock(return_value=mock_db_client)
    mock_factory = Mock(return_value=mock_client_manager)
    mock_queue_class = Mock(return_value=Mock())
    mock_notification = Mock()
    mock_notification.client_id = "client-123"
    mock_notification.session_id = "session-456"
    mock_notification.validate.return_value = True
    mock_from_dict = Mock(return_value=mock_notification)

    monkeypatch.setattr("src.server.worker.Middleware", mock_middleware_class)
    monkeypatch.setattr("src.server.worker.DatabaseClient", mock_db_class)
    monkeypatch.setattr("src.server.worker.ClientManagerFactory.create", mock_factory)
    monkeypatch.setattr("src.server.worker.multiprocessing.Queue", mock_queue_class)
    monkeypatch.setattr(
        "src.server.worker.ConnectNotification.from_dict", mock_from_dict
    )

    # Create worker and initialize
    worker = Worker(1, mock_config, mock_shared_datasets)
    worker._initialize()

    # Mock consume to capture the callback
    captured_callback = None

    def capture_callback(*args, **kwargs):
        nonlocal captured_callback
        captured_callback = kwargs["callback"]
        return "consumer-tag-123"

    mock_middleware.consume.side_effect = capture_callback

    # Call _start_consuming to register callback
    worker._start_consuming()

    # Create message body
    import json

    body = json.dumps(valid_notification_dict).encode("utf-8")

    # Call the captured callback
    captured_callback(mock_channel, mock_method, mock_properties, body)

    # Verify full flow
    mock_from_dict.assert_called_once_with(valid_notification_dict)
    mock_client_manager["handle_client"].assert_called_once_with(
        notification=mock_notification, delivery_tag=mock_method.delivery_tag
    )
