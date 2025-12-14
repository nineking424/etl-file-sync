"""Integration tests for FTP file transfer.

These tests require running FTP servers:
    docker-compose -f docker-compose.test.yml up -d
"""

import os
import tempfile
import time
from pathlib import Path

import pytest

from etl.config import ConfigLoader
from etl.transfer.ftp import FTPTransfer


# Check if FTP servers are available (environment-based)
def is_ftp_available():
    """Check if test FTP servers are running."""
    import socket

    host = os.getenv("SRC_FTP_SERVER1_HOST", "localhost")
    port = int(os.getenv("SRC_FTP_SERVER1_PORT", "21"))
    try:
        sock = socket.create_connection((host, port), timeout=2)
        sock.close()
        return True
    except (socket.error, socket.timeout):
        return False


# Mark all tests as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def require_ftp_infrastructure():
    """Ensure FTP infrastructure is available. FAIL if not."""
    if not is_ftp_available():
        pytest.fail(
            "FTP infrastructure required but not available. "
            "Run: docker-compose -f docker-compose.test.yml up -d"
        )


@pytest.fixture
def config(test_env_file):
    """Get test configuration."""
    return ConfigLoader(test_env_file)


@pytest.fixture
def source_config(config):
    """Get source FTP server config."""
    return config.get_server_config("SRC_FTP_SERVER1")


@pytest.fixture
def dest_config(config):
    """Get destination FTP server config."""
    return config.get_server_config("DST_FTP_SERVER1")


@pytest.fixture
def test_content():
    """Test file content."""
    return f"Test file content - {time.time()}"


class TestFTPConnection:
    """Test FTP connection."""

    def test_connect_source(self, source_config):
        """Test connecting to source FTP server."""
        transfer = FTPTransfer(source_config, passive_mode=True)
        transfer.connect()
        transfer.disconnect()

    def test_connect_destination(self, dest_config):
        """Test connecting to destination FTP server."""
        transfer = FTPTransfer(dest_config, passive_mode=True)
        transfer.connect()
        transfer.disconnect()

    def test_context_manager(self, source_config):
        """Test using FTP transfer as context manager."""
        with FTPTransfer(source_config, passive_mode=True) as transfer:
            assert transfer._ftp is not None

    def test_connection_error_invalid_host(self):
        """Test connection error with invalid host."""
        from etl.config import ServerConfig

        config = ServerConfig(
            hostname="INVALID",
            type="ftp",
            host="invalid.host.example",
            port=21,
            username="user",
            password="pass",
        )
        transfer = FTPTransfer(config)

        with pytest.raises(ConnectionError):
            transfer.connect()


class TestFTPUploadDownload:
    """Test FTP upload and download."""

    def test_upload_file(self, source_config, test_content):
        """Test uploading file to FTP server."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(test_content)
            local_path = f.name

        try:
            # Use absolute path with permission directory
            remote_path = f"/testserver01/upload_test_{time.time()}.txt"

            with FTPTransfer(source_config, passive_mode=True) as transfer:
                transfer.upload(local_path, remote_path)

        finally:
            os.unlink(local_path)

    def test_download_file(self, source_config, test_content):
        """Test downloading file from FTP server."""
        # First upload a file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(test_content)
            upload_path = f.name

        # Use absolute path with permission directory
        remote_path = f"/testserver01/download_test_{time.time()}.txt"

        try:
            # Upload
            with FTPTransfer(source_config, passive_mode=True) as transfer:
                transfer.upload(upload_path, remote_path)

            # Download
            with tempfile.TemporaryDirectory() as tmp_dir:
                download_path = os.path.join(tmp_dir, "downloaded.txt")

                with FTPTransfer(source_config, passive_mode=True) as transfer:
                    transfer.download(remote_path, download_path)

                # Verify content
                with open(download_path, "r") as f:
                    downloaded_content = f.read()

                assert downloaded_content == test_content

        finally:
            os.unlink(upload_path)

    def test_upload_creates_directory(self, source_config, test_content):
        """Test that upload creates remote directory if needed."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(test_content)
            local_path = f.name

        try:
            # Use absolute nested path with permission directory
            remote_path = f"/testserver01/nested/dir/structure/test_{time.time()}.txt"

            with FTPTransfer(source_config, passive_mode=True) as transfer:
                transfer.upload(local_path, remote_path)

        finally:
            os.unlink(local_path)

    def test_download_file_not_found(self, source_config):
        """Test error when downloading non-existent file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            download_path = os.path.join(tmp_dir, "should_not_exist.txt")

            with FTPTransfer(source_config, passive_mode=True) as transfer:
                with pytest.raises(FileNotFoundError):
                    transfer.download("/nonexistent_file_12345.txt", download_path)

    def test_upload_local_file_not_found(self, source_config):
        """Test error when uploading non-existent local file."""
        with FTPTransfer(source_config, passive_mode=True) as transfer:
            with pytest.raises(FileNotFoundError):
                transfer.upload("/nonexistent/local/file.txt", "/remote/file.txt")


class TestFTPTransferFlow:
    """Test complete file transfer flow (source -> destination)."""

    def test_transfer_file_between_servers(
        self, source_config, dest_config, test_content
    ):
        """Test transferring file from source to destination FTP."""
        # Create test file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(test_content)
            original_path = f.name

        timestamp = int(time.time())
        # Use absolute paths with permission directories
        source_remote = f"/testserver01/transfer_test_{timestamp}.txt"
        dest_remote = f"/testserver02/received_{timestamp}.txt"
        temp_path = None

        try:
            # 1. Upload to source FTP
            with FTPTransfer(source_config, passive_mode=True) as src:
                src.upload(original_path, source_remote)

            # 2. Download from source FTP
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                temp_path = tmp.name

            with FTPTransfer(source_config, passive_mode=True) as src:
                src.download(source_remote, temp_path)

            # 3. Upload to destination FTP
            with FTPTransfer(dest_config, passive_mode=True) as dst:
                dst.upload(temp_path, dest_remote)

            # 4. Verify by downloading from destination
            with tempfile.TemporaryDirectory() as tmp_dir:
                final_path = os.path.join(tmp_dir, "final.txt")

                with FTPTransfer(dest_config, passive_mode=True) as dst:
                    dst.download(dest_remote, final_path)

                with open(final_path, "r") as f:
                    final_content = f.read()

                assert final_content == test_content

        finally:
            os.unlink(original_path)
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)
