"""Tests for TransferFactory and BaseTransfer."""

import pytest
from unittest.mock import Mock, MagicMock

from etl.transfer.base import BaseTransfer, TransferFactory
from etl.config import ServerConfig

# Mark all tests as unit tests (no infrastructure required)
pytestmark = pytest.mark.unit


class MockTransfer(BaseTransfer):
    """Mock transfer handler for testing."""

    def __init__(self, config: ServerConfig):
        super().__init__(config)
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def download(self, remote_path: str, local_path: str) -> None:
        pass

    def upload(self, local_path: str, remote_path: str) -> None:
        pass


class MockTransfer2(BaseTransfer):
    """Another mock transfer handler for testing overwrites."""

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def download(self, remote_path: str, local_path: str) -> None:
        pass

    def upload(self, local_path: str, remote_path: str) -> None:
        pass


@pytest.fixture
def mock_config():
    """Create mock server config."""
    return ServerConfig(
        hostname="TEST_SERVER",
        type="mock",
        host="localhost",
        port=21,
        username="user",
        password="pass",
    )


@pytest.fixture
def clean_factory():
    """Clean up factory handlers before and after test."""
    # Save original handlers
    original_handlers = TransferFactory._handlers.copy()

    yield TransferFactory

    # Restore original handlers
    TransferFactory._handlers = original_handlers


class TestTransferFactory:
    """Test TransferFactory class."""

    def test_register_handler(self, clean_factory):
        """Test registering a handler."""
        clean_factory.register("mock", MockTransfer)

        assert "mock" in clean_factory._handlers
        assert clean_factory._handlers["mock"] == MockTransfer

    def test_register_overwrites(self, clean_factory):
        """Test that registering same type overwrites previous handler."""
        clean_factory.register("mock", MockTransfer)
        clean_factory.register("mock", MockTransfer2)

        assert clean_factory._handlers["mock"] == MockTransfer2

    def test_create_registered_type(self, clean_factory, mock_config):
        """Test creating handler for registered type."""
        clean_factory.register("mock", MockTransfer)

        handler = clean_factory.create(mock_config)

        assert isinstance(handler, MockTransfer)
        assert handler.config == mock_config

    def test_create_unregistered_type(self, clean_factory):
        """Test creating handler for unregistered type raises ValueError."""
        config = ServerConfig(
            hostname="TEST",
            type="unknown",
            host="localhost",
            port=21,
            username="user",
            password="pass",
        )

        with pytest.raises(ValueError, match="Unsupported transfer type: unknown"):
            clean_factory.create(config)

    def test_create_case_insensitive(self, clean_factory, mock_config):
        """Test that type matching is case-insensitive."""
        clean_factory.register("mock", MockTransfer)

        # Create with uppercase type
        config_upper = ServerConfig(
            hostname="TEST",
            type="MOCK",
            host="localhost",
            port=21,
            username="user",
            password="pass",
        )
        handler = clean_factory.create(config_upper)
        assert isinstance(handler, MockTransfer)

        # Create with mixed case type
        config_mixed = ServerConfig(
            hostname="TEST",
            type="MoCk",
            host="localhost",
            port=21,
            username="user",
            password="pass",
        )
        handler = clean_factory.create(config_mixed)
        assert isinstance(handler, MockTransfer)

    def test_error_shows_supported_types(self, clean_factory):
        """Test that error message shows supported types."""
        clean_factory.register("ftp", MockTransfer)
        clean_factory.register("sftp", MockTransfer2)

        config = ServerConfig(
            hostname="TEST",
            type="s3",
            host="localhost",
            port=21,
            username="user",
            password="pass",
        )

        with pytest.raises(ValueError) as exc_info:
            clean_factory.create(config)

        error_msg = str(exc_info.value)
        assert "Supported types:" in error_msg
        assert "ftp" in error_msg


class TestBaseTransferContextManager:
    """Test BaseTransfer context manager functionality."""

    def test_enter_calls_connect(self, mock_config):
        """Test that __enter__ calls connect()."""
        transfer = MockTransfer(mock_config)

        assert not transfer.connected

        result = transfer.__enter__()

        assert transfer.connected
        assert result is transfer

    def test_exit_calls_disconnect(self, mock_config):
        """Test that __exit__ calls disconnect()."""
        transfer = MockTransfer(mock_config)
        transfer.connect()

        assert transfer.connected

        transfer.__exit__(None, None, None)

        assert not transfer.connected

    def test_exit_on_exception(self, mock_config):
        """Test that disconnect is called even when exception occurs."""
        transfer = MockTransfer(mock_config)

        try:
            with transfer:
                assert transfer.connected
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert not transfer.connected

    def test_context_manager_full_flow(self, mock_config):
        """Test full context manager flow."""
        transfer = MockTransfer(mock_config)

        assert not transfer.connected

        with transfer as t:
            assert t is transfer
            assert transfer.connected

        assert not transfer.connected
