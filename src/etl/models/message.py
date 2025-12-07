"""Kafka message schema definitions."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4


@dataclass
class Endpoint:
    """Source or destination endpoint configuration."""

    hostname: str  # Reference to server config in .env (e.g., "SRC_FTP_SERVER1")
    path: str  # Absolute file path

    @classmethod
    def from_dict(cls, data: dict) -> "Endpoint":
        """Create Endpoint from dictionary."""
        return cls(
            hostname=data["hostname"],
            path=data["path"],
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "hostname": self.hostname,
            "path": self.path,
        }


@dataclass
class FileTransferJob:
    """File transfer job message schema.

    Example message:
    {
        "job_id": "550e8400-e29b-41d4-a716-446655440000",
        "source": {
            "hostname": "SRC_FTP_SERVER1",
            "path": "/data/input/file.csv"
        },
        "destination": {
            "hostname": "DST_FTP_SERVER1",
            "path": "/data/output/file.csv"
        }
    }
    """

    source: Endpoint
    destination: Endpoint
    job_id: str = field(default_factory=lambda: str(uuid4()))

    @classmethod
    def from_json(cls, json_str: str) -> "FileTransferJob":
        """Parse JSON string to FileTransferJob.

        Args:
            json_str: JSON string representation of the job

        Returns:
            FileTransferJob instance

        Raises:
            ValueError: If JSON is invalid or missing required fields
        """
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "FileTransferJob":
        """Create FileTransferJob from dictionary.

        Args:
            data: Dictionary with job data

        Returns:
            FileTransferJob instance

        Raises:
            ValueError: If required fields are missing
        """
        if "source" not in data:
            raise ValueError("Missing required field: source")
        if "destination" not in data:
            raise ValueError("Missing required field: destination")

        return cls(
            job_id=data.get("job_id", str(uuid4())),
            source=Endpoint.from_dict(data["source"]),
            destination=Endpoint.from_dict(data["destination"]),
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "job_id": self.job_id,
            "source": self.source.to_dict(),
            "destination": self.destination.to_dict(),
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


@dataclass
class DLQMessage:
    """Dead Letter Queue message for failed transfers.

    Example message:
    {
        "original_message": { ... },
        "error": "ConnectionRefused: ftp.source.com:21",
        "timestamp": "2025-12-08T10:30:00Z",
        "retry_count": 0
    }
    """

    original_message: dict
    error: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    retry_count: int = 0

    @classmethod
    def from_job(
        cls,
        job: FileTransferJob,
        error: str,
        retry_count: int = 0,
    ) -> "DLQMessage":
        """Create DLQ message from failed job.

        Args:
            job: The failed FileTransferJob
            error: Error message/description
            retry_count: Number of retry attempts made

        Returns:
            DLQMessage instance
        """
        return cls(
            original_message=job.to_dict(),
            error=error,
            retry_count=retry_count,
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "original_message": self.original_message,
            "error": self.error,
            "timestamp": self.timestamp,
            "retry_count": self.retry_count,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())
