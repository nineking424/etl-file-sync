"""Pytest configuration and fixtures."""

import os
import socket
import sys
from pathlib import Path

import pytest

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Load test environment
from dotenv import load_dotenv

test_env = Path(__file__).parent.parent / ".env.test"
load_dotenv(test_env)


def is_ftp_available():
    """Check if test FTP servers are running."""
    try:
        sock = socket.create_connection(("localhost", 2121), timeout=2)
        sock.close()
        return True
    except (socket.error, socket.timeout):
        return False


def is_kafka_available():
    """Check if Kafka is running."""
    try:
        sock = socket.create_connection(("localhost", 9092), timeout=2)
        sock.close()
        return True
    except (socket.error, socket.timeout):
        return False


@pytest.fixture
def test_env_file():
    """Return path to test .env file."""
    return str(test_env)


@pytest.fixture
def sample_job_json():
    """Return sample job JSON."""
    return '''
    {
        "job_id": "test-job-001",
        "source": {
            "hostname": "SRC_FTP_SERVER1",
            "path": "/test/source/file.txt"
        },
        "destination": {
            "hostname": "DST_FTP_SERVER1",
            "path": "/test/dest/file.txt"
        }
    }
    '''


@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary test file."""
    test_file = tmp_path / "test_file.txt"
    test_file.write_text("Hello, this is a test file for ETL transfer!")
    return str(test_file)


# Kafka fixtures for E2E tests
@pytest.fixture(scope="session")
def kafka_bootstrap_servers():
    """Return Kafka bootstrap servers."""
    return "localhost:9092"


@pytest.fixture
def kafka_producer(kafka_bootstrap_servers):
    """Create Kafka producer for test messages. FAIL if Kafka unavailable."""
    if not is_kafka_available():
        pytest.fail(
            "Kafka required but not available (localhost:9092). "
            "Run: docker-compose -f docker-compose.test.yml up -d"
        )

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[kafka_bootstrap_servers],
        value_serializer=lambda m: m.encode("utf-8"),
    )
    yield producer
    producer.close()


# FTP config fixtures for E2E tests
@pytest.fixture
def source_config(test_env_file):
    """Get source FTP server config."""
    from etl.config import ConfigLoader

    config = ConfigLoader(test_env_file)
    return config.get_server_config("SRC_FTP_SERVER1")


@pytest.fixture
def dest_config(test_env_file):
    """Get destination FTP server config."""
    from etl.config import ConfigLoader

    config = ConfigLoader(test_env_file)
    return config.get_server_config("DST_FTP_SERVER1")
