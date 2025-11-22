"""
Unit tests for src.server.listener.Listener

These tests validate the Listener class behaviors:
- Topology declaration (once at startup)
- Worker pool lifecycle management (start/stop/graceful shutdown)
- No message processing (workers consume directly from RabbitMQ)

All external dependencies (RabbitMQ, Worker processes) are mocked.
Tests run without Docker or any real services.
"""

import pytest
from unittest.mock import Mock, patch
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
    config.middleware_config = middleware_config

    return config


@pytest.fixture
def mock_middleware():
    """Create a mock Middleware"""
    middleware = Mock()
    middleware.connect = Mock(return_value=Mock())
    middleware.create_channel = Mock(return_value=Mock())
    middleware.declare_topology = Mock()
    middleware.close = Mock()
    return middleware


@pytest.fixture
def mock_worker_class():
    """Create a mock Worker class that returns fake worker instances"""
    fake_workers = []

    class FakeWorker:
        def __init__(self, worker_id, config, shared_datasets):
            self.worker_id = worker_id
            self.config = config
            self.shared_datasets = shared_datasets
            self.pid = 1000 + worker_id
            self._started = False
            self._terminated = False
            self._joined = False
            self._alive = True
            fake_workers.append(self)

        def start(self):
            self._started = True

        def terminate(self):
            self._terminated = True
            self._alive = False

        def join(self):
            self._joined = True

        def is_alive(self):
            return self._alive

    return FakeWorker, fake_workers


# ============================================================================
# CONSTRUCTOR TESTS
# ============================================================================


def test_listener_init(mock_config):
    """Test Listener initialization sets up all components correctly"""
    listener = Listener(mock_config)

    assert listener.config == mock_config
    assert listener.shared_datasets is None
    assert listener.workers == []


def test_listener_init_with_shared_datasets(mock_config):
    """Test Listener can be initialized with shared_datasets"""
    shared_datasets = Mock()
    listener = Listener(mock_config, shared_datasets)

    assert listener.shared_datasets is shared_datasets


# ============================================================================
# START METHOD TESTS
# ============================================================================


def test_start_declares_topology_once(
    mock_config, mock_middleware, mock_worker_class, monkeypatch
):
    """Test start declares RabbitMQ topology once before starting workers"""
    FakeWorker, fake_workers = mock_worker_class

    monkeypatch.setattr(
        "src.server.listener.Middleware", lambda config: mock_middleware
    )
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)

    listener = Listener(mock_config)
    listener._wait_for_workers = Mock()

    listener.start()

    # Verify topology declared once
    mock_middleware.declare_topology.assert_called_once()
    mock_middleware.close.assert_called_once()


def test_start_creates_workers(
    mock_config, mock_middleware, mock_worker_class, monkeypatch
):
    """Test start creates correct number of worker processes"""
    FakeWorker, fake_workers = mock_worker_class

    monkeypatch.setattr(
        "src.server.listener.Middleware", lambda config: mock_middleware
    )
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)

    listener = Listener(mock_config)
    listener._wait_for_workers = Mock()

    listener.start()

    assert len(fake_workers) == mock_config.worker_pool_size
    assert all(w._started for w in fake_workers)


def test_start_handles_keyboard_interrupt(
    mock_config, mock_middleware, mock_worker_class, monkeypatch
):
    """Test start handles KeyboardInterrupt gracefully"""
    FakeWorker, fake_workers = mock_worker_class

    monkeypatch.setattr(
        "src.server.listener.Middleware", lambda config: mock_middleware
    )
    monkeypatch.setattr("src.server.listener.Worker", FakeWorker)

    listener = Listener(mock_config)
    listener._wait_for_workers = Mock(side_effect=KeyboardInterrupt())

    listener.start()

    assert all(w._terminated for w in fake_workers)


# ============================================================================
# STOP METHOD TESTS
# ============================================================================


def test_stop_terminates_workers(mock_config, mock_worker_class):
    """Test stop sends SIGTERM to all workers"""
    FakeWorker, fake_workers = mock_worker_class

    listener = Listener(mock_config)

    for i in range(3):
        worker = FakeWorker(i, mock_config, None)
        worker._alive = True
        listener.workers.append(worker)

    listener.stop()

    assert all(w._terminated for w in fake_workers)
    assert all(w._joined for w in fake_workers)


def test_stop_without_workers(mock_config):
    """Test stop can be called without starting workers"""
    listener = Listener(mock_config)
    listener.stop()  # Should not raise
    assert listener.workers == []
