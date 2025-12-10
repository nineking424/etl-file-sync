"""Tests for message models."""

import json
import pytest

from etl.models import FileTransferJob, Endpoint, DLQMessage

# Mark all tests as unit tests (no infrastructure required)
pytestmark = pytest.mark.unit


class TestEndpoint:
    """Test Endpoint model."""

    def test_from_dict(self):
        """Test creating Endpoint from dict."""
        data = {"hostname": "TEST_SERVER", "path": "/data/file.txt"}
        endpoint = Endpoint.from_dict(data)

        assert endpoint.hostname == "TEST_SERVER"
        assert endpoint.path == "/data/file.txt"

    def test_to_dict(self):
        """Test converting Endpoint to dict."""
        endpoint = Endpoint(hostname="TEST_SERVER", path="/data/file.txt")
        result = endpoint.to_dict()

        assert result == {"hostname": "TEST_SERVER", "path": "/data/file.txt"}


class TestFileTransferJob:
    """Test FileTransferJob model."""

    def test_from_json(self, sample_job_json):
        """Test parsing JSON to FileTransferJob."""
        job = FileTransferJob.from_json(sample_job_json)

        assert job.job_id == "test-job-001"
        assert job.source.hostname == "SRC_FTP_SERVER1"
        assert job.source.path == "/test/source/file.txt"
        assert job.destination.hostname == "DST_FTP_SERVER1"
        assert job.destination.path == "/test/dest/file.txt"

    def test_from_dict(self):
        """Test creating from dict."""
        data = {
            "job_id": "job-123",
            "source": {"hostname": "SRC", "path": "/src/file.txt"},
            "destination": {"hostname": "DST", "path": "/dst/file.txt"},
        }
        job = FileTransferJob.from_dict(data)

        assert job.job_id == "job-123"
        assert job.source.hostname == "SRC"
        assert job.destination.hostname == "DST"

    def test_auto_generate_job_id(self):
        """Test auto-generating job_id when not provided."""
        data = {
            "source": {"hostname": "SRC", "path": "/file.txt"},
            "destination": {"hostname": "DST", "path": "/file.txt"},
        }
        job = FileTransferJob.from_dict(data)

        assert job.job_id is not None
        assert len(job.job_id) > 0

    def test_to_dict(self, sample_job_json):
        """Test converting to dict."""
        job = FileTransferJob.from_json(sample_job_json)
        result = job.to_dict()

        assert result["job_id"] == "test-job-001"
        assert result["source"]["hostname"] == "SRC_FTP_SERVER1"
        assert result["destination"]["hostname"] == "DST_FTP_SERVER1"

    def test_to_json(self, sample_job_json):
        """Test converting to JSON."""
        job = FileTransferJob.from_json(sample_job_json)
        json_str = job.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["job_id"] == "test-job-001"

    def test_invalid_json(self):
        """Test error on invalid JSON."""
        with pytest.raises(ValueError, match="Invalid JSON"):
            FileTransferJob.from_json("not valid json")

    def test_missing_source(self):
        """Test error when source is missing."""
        data = {"destination": {"hostname": "DST", "path": "/file.txt"}}

        with pytest.raises(ValueError, match="Missing required field: source"):
            FileTransferJob.from_dict(data)

    def test_missing_destination(self):
        """Test error when destination is missing."""
        data = {"source": {"hostname": "SRC", "path": "/file.txt"}}

        with pytest.raises(ValueError, match="Missing required field: destination"):
            FileTransferJob.from_dict(data)


class TestDLQMessage:
    """Test DLQMessage model."""

    def test_from_job(self, sample_job_json):
        """Test creating DLQ message from failed job."""
        job = FileTransferJob.from_json(sample_job_json)
        dlq = DLQMessage.from_job(job, "Connection refused")

        assert dlq.original_message["job_id"] == "test-job-001"
        assert dlq.error == "Connection refused"
        assert dlq.retry_count == 0
        assert dlq.timestamp is not None

    def test_to_dict(self, sample_job_json):
        """Test converting DLQ message to dict."""
        job = FileTransferJob.from_json(sample_job_json)
        dlq = DLQMessage.from_job(job, "Test error", retry_count=3)
        result = dlq.to_dict()

        assert "original_message" in result
        assert result["error"] == "Test error"
        assert result["retry_count"] == 3
        assert "timestamp" in result

    def test_to_json(self, sample_job_json):
        """Test converting DLQ message to JSON."""
        job = FileTransferJob.from_json(sample_job_json)
        dlq = DLQMessage.from_job(job, "Test error")
        json_str = dlq.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert "original_message" in parsed
        assert parsed["error"] == "Test error"
