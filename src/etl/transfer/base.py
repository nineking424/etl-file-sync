"""Abstract base class for file transfer implementations."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etl.config import ServerConfig


class BaseTransfer(ABC):
    """Abstract base class for file transfer operations."""

    def __init__(self, config: "ServerConfig"):
        """Initialize transfer handler.

        Args:
            config: Server configuration
        """
        self.config = config

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the server.

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the server."""
        pass

    @abstractmethod
    def download(self, remote_path: str, local_path: str) -> None:
        """Download file from remote server.

        Args:
            remote_path: Path to file on remote server
            local_path: Path to save file locally

        Raises:
            FileNotFoundError: If remote file doesn't exist
            IOError: If download fails
        """
        pass

    @abstractmethod
    def upload(self, local_path: str, remote_path: str) -> None:
        """Upload file to remote server.

        Args:
            local_path: Path to local file
            remote_path: Path to save file on remote server

        Raises:
            FileNotFoundError: If local file doesn't exist
            IOError: If upload fails
        """
        pass

    def __enter__(self) -> "BaseTransfer":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()


class TransferFactory:
    """Factory for creating transfer handlers."""

    _handlers: dict[str, type[BaseTransfer]] = {}

    @classmethod
    def register(cls, transfer_type: str, handler_class: type[BaseTransfer]) -> None:
        """Register a transfer handler.

        Args:
            transfer_type: Type identifier (e.g., "ftp", "s3")
            handler_class: Handler class to register
        """
        cls._handlers[transfer_type.lower()] = handler_class

    @classmethod
    def create(cls, config: "ServerConfig") -> BaseTransfer:
        """Create transfer handler for given config.

        Args:
            config: Server configuration

        Returns:
            Transfer handler instance

        Raises:
            ValueError: If transfer type is not supported
        """
        handler_class = cls._handlers.get(config.type.lower())
        if handler_class is None:
            supported = ", ".join(cls._handlers.keys())
            raise ValueError(
                f"Unsupported transfer type: {config.type}. "
                f"Supported types: {supported}"
            )
        return handler_class(config)
