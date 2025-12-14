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
        self._dlq_topic_suffix = os.getenv("DLQ_TOPIC_SUFFIX", "-dlq")
        self._ftp_connect_timeout = int(os.getenv("FTP_CONNECT_TIMEOUT", "30"))
        self._dlq_send_timeout = int(os.getenv("DLQ_SEND_TIMEOUT", "10"))

        # FTP Connection Pool settings
        self._ftp_pool_size = int(os.getenv("FTP_POOL_SIZE", "4"))
        self._ftp_pool_max_wait = float(os.getenv("FTP_POOL_MAX_WAIT", "30.0"))
        self._ftp_pool_idle_timeout = float(os.getenv("FTP_POOL_IDLE_TIMEOUT", "300.0"))

    @property
    def ftp_passive_mode(self) -> bool:
        """Get FTP passive mode setting."""
        return self._ftp_passive_mode

    @property
    def ftp_connect_timeout(self) -> int:
        """Get FTP connection timeout in seconds."""
        return self._ftp_connect_timeout

    @property
    def dlq_send_timeout(self) -> int:
        """Get DLQ message send timeout in seconds."""
        return self._dlq_send_timeout

    @property
    def ftp_pool_size(self) -> int:
        """Get FTP connection pool size per server."""
        return self._ftp_pool_size

    @property
    def ftp_pool_max_wait(self) -> float:
        """Get max wait time for pool connection in seconds."""
        return self._ftp_pool_max_wait

    @property
    def ftp_pool_idle_timeout(self) -> float:
        """Get pool connection idle timeout in seconds."""
        return self._ftp_pool_idle_timeout

    def get_dlq_topic(self, source_topic: str) -> str:
        """Get DLQ topic name for a given source topic.

        Args:
            source_topic: Source topic name (e.g., "mem-dft-img")

        Returns:
            DLQ topic name (e.g., "mem-dft-img-dlq")
        """
        return f"{source_topic}{self._dlq_topic_suffix}"

    def _get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean value from environment variable."""
        value = os.getenv(key, str(default)).lower()
        return value in ("true", "1", "yes", "on")

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
