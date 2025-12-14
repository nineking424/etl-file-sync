"""Unit tests for LocalTransfer handler."""

import pytest
from pathlib import Path

pytestmark = pytest.mark.unit


class TestLocalTransfer:
    """Test LocalTransfer file operations."""

    def test_download_copies_file(self, tmp_path):
        """Test download copies file from source to local."""
        from etl.transfer.local import LocalTransfer

        # Setup
        src_file = tmp_path / "source" / "test.txt"
        src_file.parent.mkdir(parents=True)
        src_file.write_text("test content")

        dst_file = tmp_path / "downloaded.txt"

        config = {"type": "local", "base_path": str(tmp_path)}

        # Execute
        with LocalTransfer(config) as transfer:
            transfer.download(str(src_file), str(dst_file))

        # Verify
        assert dst_file.exists()
        assert dst_file.read_text() == "test content"

    def test_upload_copies_file(self, tmp_path):
        """Test upload copies file from local to destination."""
        from etl.transfer.local import LocalTransfer

        # Setup
        src_file = tmp_path / "local.txt"
        src_file.write_text("upload content")

        dst_file = tmp_path / "destination" / "uploaded.txt"

        config = {"type": "local", "base_path": str(tmp_path)}

        # Execute
        with LocalTransfer(config) as transfer:
            transfer.upload(str(src_file), str(dst_file))

        # Verify
        assert dst_file.exists()
        assert dst_file.read_text() == "upload content"

    def test_upload_creates_parent_directories(self, tmp_path):
        """Test upload creates parent directories if needed."""
        from etl.transfer.local import LocalTransfer

        src_file = tmp_path / "source.txt"
        src_file.write_text("content")

        dst_file = tmp_path / "a" / "b" / "c" / "dest.txt"

        config = {"type": "local", "base_path": str(tmp_path)}

        with LocalTransfer(config) as transfer:
            transfer.upload(str(src_file), str(dst_file))

        assert dst_file.exists()

    def test_download_file_not_found(self, tmp_path):
        """Test download raises error for non-existent file."""
        from etl.transfer.local import LocalTransfer

        config = {"type": "local", "base_path": str(tmp_path)}

        with LocalTransfer(config) as transfer:
            with pytest.raises(FileNotFoundError):
                transfer.download(
                    str(tmp_path / "nonexistent.txt"),
                    str(tmp_path / "dst.txt")
                )

    def test_context_manager_no_op(self, tmp_path):
        """Test context manager works (no-op for local)."""
        from etl.transfer.local import LocalTransfer

        config = {"type": "local", "base_path": str(tmp_path)}

        # Should not raise
        with LocalTransfer(config) as transfer:
            assert transfer is not None
