"""Tests for config module."""

import os
import pytest

from etl.config import ConfigLoader, get_config, ServerConfig


class TestConfigLoader:
    """Test ConfigLoader class."""

    def test_load_env_file(self, test_env_file):
        """Test loading .env file."""
        config = ConfigLoader(test_env_file)
        assert config is not None

    def test_ftp_passive_mode_default(self, test_env_file):
        """Test FTP passive mode setting."""
        config = ConfigLoader(test_env_file)
        assert config.ftp_passive_mode is True

    def test_dlq_topic(self, test_env_file):
        """Test DLQ topic setting."""
        config = ConfigLoader(test_env_file)
        assert config.dlq_topic == "file-transfer-dlq"

    def test_get_server_config_source(self, test_env_file):
        """Test getting source server config."""
        config = ConfigLoader(test_env_file)
        server = config.get_server_config("SRC_FTP_SERVER1")

        assert server.hostname == "SRC_FTP_SERVER1"
        assert server.type == "ftp"
        assert server.host == "localhost"
        assert server.port == 2121
        assert server.username == "srcuser"
        assert server.password == "srcpass"

    def test_get_server_config_destination(self, test_env_file):
        """Test getting destination server config."""
        config = ConfigLoader(test_env_file)
        server = config.get_server_config("DST_FTP_SERVER1")

        assert server.hostname == "DST_FTP_SERVER1"
        assert server.type == "ftp"
        assert server.host == "localhost"
        assert server.port == 2122
        assert server.username == "dstuser"
        assert server.password == "dstpass"

    def test_get_server_config_not_found(self, test_env_file):
        """Test error when server config not found."""
        config = ConfigLoader(test_env_file)

        with pytest.raises(ValueError, match="Server type not found"):
            config.get_server_config("NONEXISTENT_SERVER")

    def test_get_server_config_case_insensitive(self, test_env_file):
        """Test hostname is case-insensitive."""
        config = ConfigLoader(test_env_file)

        # Should work with lowercase
        server = config.get_server_config("src_ftp_server1")
        assert server.type == "ftp"


class TestServerConfig:
    """Test ServerConfig dataclass."""

    def test_server_config_creation(self):
        """Test creating ServerConfig."""
        config = ServerConfig(
            hostname="TEST_SERVER",
            type="ftp",
            host="example.com",
            port=21,
            username="user",
            password="pass",
        )

        assert config.hostname == "TEST_SERVER"
        assert config.type == "ftp"
        assert config.host == "example.com"
        assert config.port == 21
        assert config.username == "user"
        assert config.password == "pass"
