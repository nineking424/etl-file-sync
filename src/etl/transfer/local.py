"""Local filesystem transfer handler."""

import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Union

from .base import BaseTransfer, TransferFactory

if TYPE_CHECKING:
    from etl.config import ServerConfig

logger = logging.getLogger(__name__)


class LocalTransfer(BaseTransfer):
    """Transfer handler for local filesystem operations.

    This handler copies files between local paths on the filesystem.
    It's useful for:
    - Container volume-mounted directories
    - Local file system operations
    - Testing without network dependencies
    """

    def __init__(self, config: Union["ServerConfig", dict], **kwargs):
        """Initialize local transfer handler.

        Args:
            config: Server configuration (ServerConfig or dict)
                    For dict, expects: {"type": "local", "base_path": "/path"}
        """
        # Support both ServerConfig and dict for flexibility
        if isinstance(config, dict):
            self.config = config
            self.base_path = config.get("base_path", "")
        else:
            super().__init__(config)
            # ServerConfig doesn't have base_path, use host field or empty
            self.base_path = getattr(config, "base_path", "") or ""

    def connect(self) -> None:
        """No connection needed for local filesystem."""
        logger.debug("LocalTransfer: No connection needed (local filesystem)")

    def disconnect(self) -> None:
        """No disconnection needed for local filesystem."""
        logger.debug("LocalTransfer: No disconnection needed (local filesystem)")

    def download(self, remote_path: str, local_path: str) -> None:
        """Copy file from source path to destination path.

        For LocalTransfer, "download" means copy from source to destination.

        Args:
            remote_path: Source file path
            local_path: Destination file path

        Raises:
            FileNotFoundError: If source file doesn't exist
        """
        src = Path(remote_path)
        if not src.exists():
            raise FileNotFoundError(f"Source file not found: {remote_path}")

        dst = Path(local_path)
        dst.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"LocalTransfer download: {remote_path} -> {local_path}")
        shutil.copy2(src, dst)
        logger.info(f"LocalTransfer download complete: {dst.stat().st_size} bytes")

    def upload(self, local_path: str, remote_path: str) -> None:
        """Copy file from source path to destination path.

        For LocalTransfer, "upload" means copy from source to destination.

        Args:
            local_path: Source file path
            remote_path: Destination file path

        Raises:
            FileNotFoundError: If source file doesn't exist
        """
        src = Path(local_path)
        if not src.exists():
            raise FileNotFoundError(f"Source file not found: {local_path}")

        dst = Path(remote_path)
        dst.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"LocalTransfer upload: {local_path} -> {remote_path}")
        shutil.copy2(src, dst)
        logger.info(f"LocalTransfer upload complete: {dst.stat().st_size} bytes")

    def __enter__(self) -> "LocalTransfer":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit."""
        self.disconnect()
        return False


# Register local handler with factory
TransferFactory.register("local", LocalTransfer)
