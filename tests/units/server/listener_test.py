"""
Unit tests for src.server.listener.Listener

These tests validate all behaviors of the Listener class:
- Message callback handling (valid/invalid notifications)
- Start/stop lifecycle
- Worker pool management
- Queue interactions
- Error handling

All external dependencies (RabbitMQ, Worker processes) are mocked.
Tests run without Docker or any real services.
"""

import json
import pytest
from unittest.mock import Mock, MagicMock, call, patch
from src.server.listener import Listener
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
def mock_middleware():
    """Create a mock RabbitMQMiddleware"""
    middleware = Mock()
    middleware.connect = Mock(return_value=Mock())
    middleware.create_channel = Mock(return_value=Mock())
    middleware.declare_topology = Mock()
    middleware.consume = Mock(return_value="test-consumer-tag")
    middleware.start_consuming = Mock()
    middleware.stop_consuming = Mock()
    middleware.close = Mock()
    middleware.ack_message = Mock()
    middleware.nack_message = Mock()
    return middleware


@pytest.fixture
def mock_worker_class():
    """Create a mock Worker class that returns fake worker instances"""
    fake_workers = []
    
    class FakeWorker:
        def __init__(self, worker_id, config, jobs_queue, shared_datasets):
            self.worker_id = worker_id
            self.config = config
            self.jobs_queue = jobs_queue
            self.shared_datasets = shared_datasets
            self.pid = 1000 + worker_id
            self._started = False
            self._terminated = False
            self._joined = False
            fake_workers.append(self)
        
        def start(self):
            self._started = True
        
        def terminate(self):
            self._terminated = True
        
        def join(self):
            self._joined = True
    
    return FakeWorker, fake_workers


@pytest.fixture
def mock_queue():
    """Create a mock multiprocessing.Queue"""
    queue = Mock()
    queue.put = Mock()
    queue.get = Mock()
    return queue


@pytest.fixture
def mock_shared_datasets():
    """Create a mock shared datasets object"""
    return Mock()


# ============================================================================
# CONSTRUCTOR TESTS
# ============================================================================


def test_listener_init(mock_config, monkeypatch):
    """Test Listener initialization sets up all components correctly"""
    mock_middleware_class = Mock()
    mock_queue_class = Mock(return_value=Mock())
    
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", mock_middleware_class)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", mock_queue_class)
    
    shared_datasets = Mock()
    listener = Listener(mock_config, shared_datasets)
    
    # Verify initialization
    assert listener.config == mock_config
    assert listener.shared_datasets == shared_datasets
    assert listener.consumer_tag is None
    assert listener.workers == []
    
    # Verify middleware was created with correct config
    mock_middleware_class.assert_called_once_with(mock_config.middleware_config)
    
    # Verify jobs_queue was created with correct maxsize
    mock_queue_class.assert_called_once_with(maxsize=mock_config.worker_pool_size + 10)


# ============================================================================
# START METHOD TESTS
# ============================================================================


def test_start_success(mock_config, mock_middleware, mock_worker_class, mock_queue, monkeypatch):
    """Test successful start: connects, declares topology, starts workers, begins consuming"""
    FakeWorker, fake_workers = mock_worker_class
    
    # Patch dependencies
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Prevent blocking in start_consuming
    mock_middleware.start_consuming = Mock()
    
    # Call start
    listener.start()
    
    # Verify connection flow
    mock_middleware.connect.assert_called_once()
    mock_middleware.create_channel.assert_called_once()
    
    # Verify topology declaration
    mock_middleware.declare_topology.assert_called_once()
    call_args = mock_middleware.declare_topology.call_args
    assert call_args[1]["prefetch_count"] == mock_config.worker_pool_size
    
    # Verify workers were created and started
    assert len(fake_workers) == mock_config.worker_pool_size
    for i, worker in enumerate(fake_workers):
        assert worker.worker_id == i
        assert worker._started is True
        assert worker.config == mock_config
        assert worker.jobs_queue == mock_queue
    
    # Verify consume was called with correct parameters
    mock_middleware.consume.assert_called_once()
    call_args = mock_middleware.consume.call_args
    assert call_args[1]["queue"] == "generate_data_queue"
    assert call_args[1]["auto_ack"] is False
    assert call_args[1]["consumer_tag"] == mock_config.pod_name
    assert callable(call_args[1]["callback"])
    
    # Verify consumer_tag was stored
    assert listener.consumer_tag == "test-consumer-tag"
    
    # Verify start_consuming was called
    mock_middleware.start_consuming.assert_called_once()


def test_start_with_keyboard_interrupt(mock_config, mock_middleware, mock_worker_class, mock_queue, monkeypatch):
    """Test start handles KeyboardInterrupt gracefully"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    # Simulate KeyboardInterrupt during start_consuming
    mock_middleware.start_consuming = Mock(side_effect=KeyboardInterrupt())
    
    listener = Listener(mock_config)
    
    # Should not raise, just log
    listener.start()
    
    # Verify it got to start_consuming
    mock_middleware.start_consuming.assert_called_once()


def test_start_with_exception(mock_config, mock_middleware, mock_worker_class, mock_queue, monkeypatch):
    """Test start handles exceptions and calls stop"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    # Simulate exception during start_consuming
    error = RuntimeError("Connection lost")
    mock_middleware.start_consuming = Mock(side_effect=error)
    
    listener = Listener(mock_config)
    
    # Should raise the exception after calling stop
    with pytest.raises(RuntimeError, match="Connection lost"):
        listener.start()
    
    # Verify stop was called (workers terminated)
    assert all(w._terminated for w in fake_workers)


# ============================================================================
# CALLBACK TESTS - VALID NOTIFICATION
# ============================================================================


def test_callback_valid_notification(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with valid notification adds job to queue"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    mock_middleware.consume = Mock(return_value="test-consumer-tag")
    listener.start = Mock()  # Don't actually start
    
    # Manually trigger consume to capture callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Prepare valid notification
    valid_notification = {
        "client_id": "client-123",
        "session_id": "session-456",
        "inputs_format": "image",
        "outputs_format": "label",
        "model_type": "segmentation"
    }
    
    # Create fake RabbitMQ message
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 42
    fake_properties = Mock()
    body = json.dumps(valid_notification).encode("utf-8")
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was added to queue
    mock_queue.put.assert_called_once()
    job = mock_queue.put.call_args[0][0]
    
    assert job["notification"] == valid_notification
    assert job["delivery_tag"] == 42
    
    # Verify NO ack/nack was called (worker will handle ACK)
    mock_middleware.ack_message.assert_not_called()
    mock_middleware.nack_message.assert_not_called()


def test_callback_valid_notification_minimal_fields(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with valid notification containing only required fields"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Prepare minimal valid notification (only required fields)
    minimal_notification = {
        "client_id": "client-999",
        "session_id": "session-888"
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 99
    fake_properties = Mock()
    body = json.dumps(minimal_notification).encode("utf-8")
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was added to queue
    mock_queue.put.assert_called_once()
    job = mock_queue.put.call_args[0][0]
    assert job["notification"]["client_id"] == "client-999"
    assert job["notification"]["session_id"] == "session-888"


# ============================================================================
# CALLBACK TESTS - INVALID JSON
# ============================================================================


def test_callback_invalid_json(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with invalid JSON nacks message with requeue=False"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Prepare invalid JSON
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 100
    fake_properties = Mock()
    body = b"{ this is not valid json }"
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was NOT added to queue
    mock_queue.put.assert_not_called()
    
    # Verify message was nacked with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=fake_channel,
        delivery_tag=100,
        requeue=False
    )


# ============================================================================
# CALLBACK TESTS - INVALID NOTIFICATION FIELDS
# ============================================================================


def test_callback_missing_client_id(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with missing client_id nacks message with requeue=False"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Missing client_id
    invalid_notification = {
        "session_id": "session-456"
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 200
    fake_properties = Mock()
    body = json.dumps(invalid_notification).encode("utf-8")
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was NOT added to queue
    mock_queue.put.assert_not_called()
    
    # Verify message was nacked with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=fake_channel,
        delivery_tag=200,
        requeue=False
    )


def test_callback_missing_session_id(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with missing session_id nacks message with requeue=False"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Missing session_id
    invalid_notification = {
        "client_id": "client-123"
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 201
    fake_properties = Mock()
    body = json.dumps(invalid_notification).encode("utf-8")
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was NOT added to queue
    mock_queue.put.assert_not_called()
    
    # Verify message was nacked with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=fake_channel,
        delivery_tag=201,
        requeue=False
    )


def test_callback_empty_client_id(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with empty client_id nacks message with requeue=False"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Empty client_id
    invalid_notification = {
        "client_id": "",
        "session_id": "session-456"
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 202
    fake_properties = Mock()
    body = json.dumps(invalid_notification).encode("utf-8")
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was NOT added to queue
    mock_queue.put.assert_not_called()
    
    # Verify message was nacked with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=fake_channel,
        delivery_tag=202,
        requeue=False
    )


def test_callback_null_values(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback with null required fields nacks message with requeue=False"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Null values
    invalid_notification = {
        "client_id": None,
        "session_id": None
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 203
    fake_properties = Mock()
    body = json.dumps(invalid_notification).encode("utf-8")
    
    # Call the callback
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was NOT added to queue
    mock_queue.put.assert_not_called()
    
    # Verify message was nacked with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=fake_channel,
        delivery_tag=203,
        requeue=False
    )


# ============================================================================
# CALLBACK TESTS - EXCEPTION HANDLING
# ============================================================================


def test_callback_exception_during_processing(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test callback handles exceptions and nacks with requeue=False"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture the callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Make queue.put raise an exception
    mock_queue.put = Mock(side_effect=RuntimeError("Queue full"))
    
    # Valid notification
    valid_notification = {
        "client_id": "client-123",
        "session_id": "session-456"
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 300
    fake_properties = Mock()
    body = json.dumps(valid_notification).encode("utf-8")
    
    # Call the callback - should not raise
    callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify message was nacked with requeue=False
    mock_middleware.nack_message.assert_called_once_with(
        channel=fake_channel,
        delivery_tag=300,
        requeue=False
    )


# ============================================================================
# STOP METHOD TESTS
# ============================================================================


def test_stop_success(mock_config, mock_middleware, mock_worker_class, mock_queue, monkeypatch):
    """Test stop gracefully shuts down all workers and middleware"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Manually set up workers and consumer_tag as if start() was called
    listener.consumer_tag = "test-consumer-tag"
    for i in range(3):
        worker = FakeWorker(i, mock_config, mock_queue, None)
        worker.start()
        listener.workers.append(worker)
    
    # Call stop
    listener.stop()
    
    # Verify stop_consuming was called with correct consumer_tag
    mock_middleware.stop_consuming.assert_called_once_with("test-consumer-tag")
    
    # Verify None signals were sent to workers
    assert mock_queue.put.call_count == 3
    for call_args in mock_queue.put.call_args_list:
        assert call_args[0][0] is None
    
    # Verify all workers were terminated
    for worker in fake_workers:
        assert worker._terminated is True
    
    # Verify all workers were joined
    for worker in fake_workers:
        assert worker._joined is True
    
    # Verify middleware was closed
    mock_middleware.close.assert_called_once()


def test_stop_without_start(mock_config, mock_middleware, monkeypatch):
    """Test stop can be called even if start was never called"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    
    listener = Listener(mock_config)
    
    # Call stop without starting - should not raise
    listener.stop()
    
    # Verify stop_consuming was called with None
    mock_middleware.stop_consuming.assert_called_once_with(None)
    
    # Verify middleware was closed
    mock_middleware.close.assert_called_once()


# ============================================================================
# CONSUME_MESSAGES TESTS
# ============================================================================


def test_consume_messages_sets_consumer_tag(mock_config, mock_middleware, monkeypatch):
    """Test _consume_messages stores the consumer_tag returned by middleware"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    
    listener = Listener(mock_config)
    
    mock_middleware.consume = Mock(return_value="custom-consumer-tag")
    mock_middleware.start_consuming = Mock()
    
    fake_channel = Mock()
    listener._consume_messages(fake_channel)
    
    # Verify consumer_tag was stored
    assert listener.consumer_tag == "custom-consumer-tag"


def test_consume_messages_uses_pod_name_as_consumer_tag(mock_config, mock_middleware, monkeypatch):
    """Test _consume_messages passes pod_name as consumer_tag"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    
    listener = Listener(mock_config)
    
    mock_middleware.consume = Mock(return_value="test-tag")
    mock_middleware.start_consuming = Mock()
    
    fake_channel = Mock()
    listener._consume_messages(fake_channel)
    
    # Verify consume was called with pod_name as consumer_tag
    call_args = mock_middleware.consume.call_args[1]
    assert call_args["consumer_tag"] == mock_config.pod_name


def test_consume_messages_calls_start_consuming(mock_config, mock_middleware, monkeypatch):
    """Test _consume_messages calls start_consuming on middleware"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    
    listener = Listener(mock_config)
    
    mock_middleware.start_consuming = Mock()
    
    fake_channel = Mock()
    listener._consume_messages(fake_channel)
    
    # Verify start_consuming was called with the channel
    mock_middleware.start_consuming.assert_called_once_with(fake_channel)


# ============================================================================
# INTERRUPT_WORKERS TESTS
# ============================================================================


def test_interrupt_workers_sends_none_signals(mock_config, mock_worker_class, mock_queue, monkeypatch):
    """Test _interrupt_workers sends None signals to all workers"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    
    listener = Listener(mock_config)
    listener.jobs_queue = mock_queue
    
    # Add workers
    for i in range(3):
        worker = FakeWorker(i, mock_config, mock_queue, None)
        listener.workers.append(worker)
    
    # Call _interrupt_workers
    listener._interrupt_workers()
    
    # Verify None signals sent
    assert mock_queue.put.call_count == 3
    for call_args in mock_queue.put.call_args_list:
        assert call_args[0][0] is None


def test_interrupt_workers_terminates_all_workers(mock_config, mock_worker_class, mock_queue, monkeypatch):
    """Test _interrupt_workers terminates all workers"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    
    listener = Listener(mock_config)
    listener.jobs_queue = mock_queue
    
    # Add workers
    for i in range(3):
        worker = FakeWorker(i, mock_config, mock_queue, None)
        listener.workers.append(worker)
    
    # Call _interrupt_workers
    listener._interrupt_workers()
    
    # Verify all workers terminated
    for worker in fake_workers:
        assert worker._terminated is True


def test_interrupt_workers_joins_all_workers(mock_config, mock_worker_class, mock_queue, monkeypatch):
    """Test _interrupt_workers joins all workers"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    
    listener = Listener(mock_config)
    listener.jobs_queue = mock_queue
    
    # Add workers
    for i in range(3):
        worker = FakeWorker(i, mock_config, mock_queue, None)
        listener.workers.append(worker)
    
    # Call _interrupt_workers
    listener._interrupt_workers()
    
    # Verify all workers joined
    for worker in fake_workers:
        assert worker._joined is True


# ============================================================================
# INTEGRATION-STYLE TESTS (still unit tests, no real services)
# ============================================================================


def test_full_lifecycle(mock_config, mock_middleware, mock_worker_class, mock_queue, monkeypatch):
    """Test complete start -> process message -> stop lifecycle"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    # Don't block in start_consuming
    captured_callback = None
    
    def capture_consume(**kwargs):
        nonlocal captured_callback
        captured_callback = kwargs["callback"]
        return "test-consumer-tag"
    
    mock_middleware.consume = Mock(side_effect=capture_consume)
    mock_middleware.start_consuming = Mock()
    
    # Create listener and start
    listener = Listener(mock_config)
    listener.start()
    
    # Verify workers started
    assert len(fake_workers) == 3
    assert all(w._started for w in fake_workers)
    
    # Simulate incoming message
    valid_notification = {
        "client_id": "client-abc",
        "session_id": "session-xyz"
    }
    
    fake_channel = Mock()
    fake_method = Mock()
    fake_method.delivery_tag = 500
    fake_properties = Mock()
    body = json.dumps(valid_notification).encode("utf-8")
    
    captured_callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify job was queued
    mock_queue.put.assert_called()
    job = mock_queue.put.call_args[0][0]
    assert job["notification"]["client_id"] == "client-abc"
    assert job["delivery_tag"] == 500
    
    # Stop listener
    listener.stop()
    
    # Verify clean shutdown
    assert all(w._terminated for w in fake_workers)
    assert all(w._joined for w in fake_workers)
    mock_middleware.stop_consuming.assert_called_once()
    mock_middleware.close.assert_called_once()


def test_multiple_valid_messages_queued(mock_config, mock_middleware, mock_queue, monkeypatch):
    """Test multiple valid messages are correctly queued"""
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    listener = Listener(mock_config)
    
    # Capture callback
    listener._consume_messages(Mock())
    callback = mock_middleware.consume.call_args[1]["callback"]
    
    # Send multiple messages
    notifications = [
        {"client_id": f"client-{i}", "session_id": f"session-{i}"}
        for i in range(5)
    ]
    
    for i, notification in enumerate(notifications):
        fake_channel = Mock()
        fake_method = Mock()
        fake_method.delivery_tag = 1000 + i
        fake_properties = Mock()
        body = json.dumps(notification).encode("utf-8")
        
        callback(fake_channel, fake_method, fake_properties, body)
    
    # Verify all jobs queued
    assert mock_queue.put.call_count == 5
    
    # Verify jobs have correct data
    for i, call_args in enumerate(mock_queue.put.call_args_list):
        job = call_args[0][0]
        assert job["notification"]["client_id"] == f"client-{i}"
        assert job["delivery_tag"] == 1000 + i


def test_shared_datasets_passed_to_workers(mock_config, mock_middleware, mock_worker_class, mock_queue, monkeypatch):
    """Test shared_datasets is correctly passed to worker instances"""
    FakeWorker, fake_workers = mock_worker_class
    
    monkeypatch.setattr("src.server.listener.RabbitMQMiddleware", lambda config: mock_middleware)
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)
    monkeypatch.setattr("src.server.listener.multiprocessing.Queue", lambda maxsize: mock_queue)
    
    mock_shared = Mock()
    listener = Listener(mock_config, shared_datasets=mock_shared)
    
    mock_middleware.start_consuming = Mock()
    listener.start()
    
    # Verify all workers received shared_datasets
    for worker in fake_workers:
        assert worker.shared_datasets is mock_shared
