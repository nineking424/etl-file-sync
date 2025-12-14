"""Tests for config module."""

import os
import pytest

from etl.config import ConfigLoader, get_config, ServerConfig

# Mark all tests as unit tests (no infrastructure required)
pytestmark = pytest.mark.unit


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

    def test_get_dlq_topic_default_suffix(self, test_env_file):
        """Test DLQ topic generation with default suffix."""
        config = ConfigLoader(test_env_file)
        assert config.get_dlq_topic("mem-dft-img") == "mem-dft-img-dlq"
        assert config.get_dlq_topic("fdry-cdsem-img") == "fdry-cdsem-img-dlq"

    def test_get_dlq_topic_custom_suffix(self, monkeypatch):
        """Test DLQ topic generation with custom suffix."""
        monkeypatch.setenv("DLQ_TOPIC_SUFFIX", ".failed")
        config = ConfigLoader()
        assert config.get_dlq_topic("test-topic") == "test-topic.failed"

    def test_get_dlq_topic_empty_suffix(self, monkeypatch):
        """Test DLQ topic generation with empty suffix."""
        monkeypatch.setenv("DLQ_TOPIC_SUFFIX", "")
        config = ConfigLoader()
        assert config.get_dlq_topic("test-topic") == "test-topic"

    # Boundary value tests for get_dlq_topic
    def test_get_dlq_topic_with_special_characters(self, test_env_file):
        """Test DLQ topic with special characters in topic name."""
        config = ConfigLoader(test_env_file)
        assert config.get_dlq_topic("topic-with-dashes") == "topic-with-dashes-dlq"
        assert config.get_dlq_topic("topic.with.dots") == "topic.with.dots-dlq"
        assert config.get_dlq_topic("topic_with_underscores") == "topic_with_underscores-dlq"

    def test_get_dlq_topic_with_long_name(self, test_env_file):
        """Test DLQ topic with very long topic name."""
        config = ConfigLoader(test_env_file)
        long_topic = "a" * 200
        result = config.get_dlq_topic(long_topic)
        assert result == f"{long_topic}-dlq"
        assert len(result) == 204

    def test_get_dlq_topic_with_single_char(self, test_env_file):
        """Test DLQ topic with single character topic name."""
        config = ConfigLoader(test_env_file)
        assert config.get_dlq_topic("x") == "x-dlq"

    def test_get_server_config_source(self, test_env_file):
        """Test getting source server config."""
        config = ConfigLoader(test_env_file)
        server = config.get_server_config("SRC_FTP_SERVER1")

        assert server.hostname == "SRC_FTP_SERVER1"
        assert server.type == "ftp"
        assert server.host == "192.168.1.4"
        assert server.port == 21
        assert server.username == "user01"
        assert server.password == "Votmdnjem01!"

    def test_get_server_config_destination(self, test_env_file):
        """Test getting destination server config."""
        config = ConfigLoader(test_env_file)
        server = config.get_server_config("DST_FTP_SERVER1")

        assert server.hostname == "DST_FTP_SERVER1"
        assert server.type == "ftp"
        assert server.host == "192.168.1.4"
        assert server.port == 21
        assert server.username == "user02"
        assert server.password == "Votmdnjem02!"

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
