"""Configuration loader for server credentials from environment variables."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class ServerConfig:
    """Server connection configuration."""

    hostname: str
    type: str  # ftp, s3 (future)
    host: str
    port: int
    username: str
    password: str


class ConfigLoader:
    """Load server configurations from environment variables."""

    def __init__(self, env_file: Optional[str] = None):
        """Initialize configuration loader.

        Args:
            env_file: Path to .env file. If None, uses default .env location.
        """
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()

        self._ftp_passive_mode = self._get_bool("FTP_PASSIVE_MODE", True)
        self._dlq_topic = os.getenv("DLQ_TOPIC", "file-transfer-dlq")
        self._worker_count = self._get_int("ETL_WORKER_COUNT", 1)
        self._topic_partitions = self._get_int("KAFKA_TOPIC_PARTITIONS", 1)
        self._worker_id = os.getenv("ETL_WORKER_ID")

    @property
    def ftp_passive_mode(self) -> bool:
        """Get FTP passive mode setting."""
        return self._ftp_passive_mode

    @property
    def dlq_topic(self) -> str:
        """Get DLQ topic name."""
        return self._dlq_topic

    @property
    def worker_count(self) -> int:
        """Get ETL worker count."""
        return self._worker_count

    @property
    def topic_partitions(self) -> int:
        """Get Kafka topic partitions count."""
        return self._topic_partitions

    @property
    def worker_id(self) -> Optional[str]:
        """Get ETL worker ID (set by supervisor for each process)."""
        return self._worker_id

    def _get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean value from environment variable."""
        value = os.getenv(key, str(default)).lower()
        return value in ("true", "1", "yes", "on")

    def _get_int(self, key: str, default: int = 0) -> int:
        """Get integer value from environment variable."""
        value = os.getenv(key, str(default))
        try:
            return int(value)
        except ValueError:
            return default

    def get_server_config(self, hostname: str) -> ServerConfig:
        """Get server configuration by hostname.

        Args:
            hostname: Server hostname identifier (e.g., "SRC_FTP_SERVER1")

        Returns:
            ServerConfig with connection details

        Raises:
            ValueError: If required configuration is missing
        """
        prefix = hostname.upper()

        server_type = os.getenv(f"{prefix}_TYPE")
        if not server_type:
            raise ValueError(f"Server type not found for hostname: {hostname}")

        host = os.getenv(f"{prefix}_HOST")
        if not host:
            raise ValueError(f"Host not found for hostname: {hostname}")

        port_str = os.getenv(f"{prefix}_PORT", "21")
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid port for hostname {hostname}: {port_str}")

        username = os.getenv(f"{prefix}_USER", "")
        password = os.getenv(f"{prefix}_PASS", "")

        return ServerConfig(
            hostname=hostname,
            type=server_type.lower(),
            host=host,
            port=port,
            username=username,
            password=password,
        )


# Global config instance
_config: Optional[ConfigLoader] = None


def get_config(env_file: Optional[str] = None) -> ConfigLoader:
    """Get or create the global configuration loader.

    Args:
        env_file: Path to .env file (only used on first call)

    Returns:
        ConfigLoader instance
    """
    global _config
    if _config is None:
        _config = ConfigLoader(env_file)
    return _config
