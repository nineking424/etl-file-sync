"""Infrastructure health check tests.

These tests verify that required infrastructure is available.
They MUST fail (not skip) if infrastructure is unavailable.

Usage:
    pytest tests/test_health.py -v
"""

import socket

import pytest


def check_connection(host: str, port: int, timeout: float = 5.0) -> None:
    """Assert that a service is reachable. Raises on failure."""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
    except (socket.error, socket.timeout) as e:
        pytest.fail(f"Cannot connect to {host}:{port} - {e}")


@pytest.mark.integration
class TestInfrastructureHealth:
    """Infrastructure health checks - FAIL if services unavailable."""

    def test_ftp_source_available(self):
        """FTP source server must be reachable."""
        check_connection("localhost", 2121)

    def test_ftp_dest_available(self):
        """FTP destination server must be reachable."""
        check_connection("localhost", 2122)


@pytest.mark.e2e
class TestKafkaHealth:
    """Kafka health check - FAIL if unavailable."""

    def test_kafka_available(self):
        """Kafka broker must be reachable."""
        check_connection("localhost", 9092)
