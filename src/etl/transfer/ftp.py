"""FTP file transfer implementation."""

import ftplib
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from .base import BaseTransfer, TransferFactory

if TYPE_CHECKING:
    from etl.config import ServerConfig

logger = logging.getLogger(__name__)


class FTPTransfer(BaseTransfer):
    """FTP file transfer handler."""

    def __init__(
        self, config: "ServerConfig", passive_mode: bool = True, timeout: int = 30
    ):
        """Initialize FTP transfer handler.

        Args:
            config: Server configuration
            passive_mode: Use passive mode (default: True)
            timeout: Connection timeout in seconds (default: 30)
        """
        super().__init__(config)
        self.passive_mode = passive_mode
        self.timeout = timeout
        self._ftp: ftplib.FTP | None = None

    def connect(self) -> None:
        """Establish FTP connection.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            logger.info(
                f"Connecting to FTP server: {self.config.host}:{self.config.port}"
            )

            self._ftp = ftplib.FTP()
            self._ftp.connect(self.config.host, self.config.port, timeout=self.timeout)
            self._ftp.login(self.config.username, self.config.password)

            # Set passive/active mode
            self._ftp.set_pasv(self.passive_mode)
            mode = "passive" if self.passive_mode else "active"
            logger.info(f"FTP connection established ({mode} mode)")

        except ftplib.all_errors as e:
            raise ConnectionError(
                f"Failed to connect to FTP server "
                f"{self.config.host}:{self.config.port}: {e}"
            )

    def disconnect(self) -> None:
        """Close FTP connection."""
        if self._ftp:
            try:
                self._ftp.quit()
                logger.info("FTP connection closed")
            except ftplib.all_errors:
                # Force close if quit fails
                self._ftp.close()
            finally:
                self._ftp = None

    def download(self, remote_path: str, local_path: str) -> None:
        """Download file from FTP server.

        Args:
            remote_path: Path to file on FTP server
            local_path: Path to save file locally

        Raises:
            ConnectionError: If not connected
            FileNotFoundError: If remote file doesn't exist
            IOError: If download fails
        """
        if not self._ftp:
            raise ConnectionError("Not connected to FTP server")

        logger.info(f"Downloading: {remote_path} -> {local_path}")

        # Ensure local directory exists
        local_dir = Path(local_path).parent
        local_dir.mkdir(parents=True, exist_ok=True)

        try:
            with open(local_path, "wb") as f:
                self._ftp.retrbinary(f"RETR {remote_path}", f.write)

            file_size = os.path.getsize(local_path)
            logger.info(f"Download complete: {file_size} bytes")

        except ftplib.error_perm as e:
            error_msg = str(e)
            if "550" in error_msg:  # File not found
                raise FileNotFoundError(f"Remote file not found: {remote_path}")
            raise IOError(f"FTP download failed: {e}")

        except ftplib.all_errors as e:
            raise IOError(f"FTP download failed: {e}")

    def upload(self, local_path: str, remote_path: str) -> None:
        """Upload file to FTP server.

        Args:
            local_path: Path to local file
            remote_path: Path to save file on FTP server

        Raises:
            ConnectionError: If not connected
            FileNotFoundError: If local file doesn't exist
            IOError: If upload fails
        """
        if not self._ftp:
            raise ConnectionError("Not connected to FTP server")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        logger.info(f"Uploading: {local_path} -> {remote_path}")

        try:
            # Ensure remote directory exists
            remote_dir = str(Path(remote_path).parent)
            self._ensure_remote_dir(remote_dir)

            with open(local_path, "rb") as f:
                self._ftp.storbinary(f"STOR {remote_path}", f)

            file_size = os.path.getsize(local_path)
            logger.info(f"Upload complete: {file_size} bytes")

        except ftplib.all_errors as e:
            raise IOError(f"FTP upload failed: {e}")

    def _ensure_remote_dir(self, remote_dir: str) -> None:
        """Ensure remote directory exists, creating if necessary.

        Args:
            remote_dir: Remote directory path
        """
        if not remote_dir or remote_dir == "/":
            return

        # Split path and create directories recursively
        parts = remote_dir.strip("/").split("/")
        current_path = ""

        for part in parts:
            current_path = f"{current_path}/{part}"
            try:
                self._ftp.cwd(current_path)
            except ftplib.error_perm:
                try:
                    self._ftp.mkd(current_path)
                    logger.debug(f"Created remote directory: {current_path}")
                except ftplib.error_perm:
                    # Directory might already exist or permission denied
                    pass

        # Return to root
        self._ftp.cwd("/")


# Register FTP handler with factory
TransferFactory.register("ftp", FTPTransfer)
