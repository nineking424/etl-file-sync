"""End-to-end integration tests.

Test flow:
1. Test code -> uploads file to source FTP
2. Test code -> sends transfer job message to Kafka
3. Container Consumer -> receives message -> performs FTP transfer
4. Test code -> verifies file on destination FTP

Prerequisites:
    docker-compose -f docker-compose.test.yml up -d
"""

import json
import os
import tempfile
import time
from typing import Callable

import pytest

from .conftest import is_ftp_available, is_kafka_available


# E2E Test Configuration
E2E_TIMEOUT = 3.0  # Maximum wait time for quick operations (DLQ, etc.)
E2E_TRANSFER_TIMEOUT = 15.0  # Maximum wait time for file transfers (includes Kafka consumer lag + FTP latency)
E2E_POLL_INTERVAL = 0.5  # Poll interval in seconds


def wait_for_condition(
    check_func: Callable[[], bool],
    timeout: float = E2E_TIMEOUT,
    interval: float = E2E_POLL_INTERVAL,
) -> bool:
    """Wait for a condition to be true with polling.

    Args:
        check_func: Function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds

    Returns:
        True if condition was met, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        if check_func():
            return True
        time.sleep(interval)
    return False


# Mark all tests as E2E tests
pytestmark = pytest.mark.e2e


@pytest.fixture(autouse=True)
def require_full_infrastructure():
    """Ensure full infrastructure is available. FAIL if not."""
    import os

    ftp_host = os.getenv("SRC_FTP_SERVER1_HOST", "localhost")
    ftp_port = os.getenv("SRC_FTP_SERVER1_PORT", "21")

    missing = []
    if not is_ftp_available():
        missing.append(f"FTP ({ftp_host}:{ftp_port})")
    if not is_kafka_available():
        missing.append("Kafka (localhost:9092)")

    if missing:
        pytest.fail(
            f"Infrastructure required but not available: {', '.join(missing)}. "
            "Run: docker-compose -f docker-compose.test.yml up -d"
        )



class TestEndToEndTransfer:
    """Full pipeline tests: Kafka -> Container Consumer -> FTP."""

    def test_full_transfer_pipeline(
        self, test_env_file, kafka_producer, source_config, dest_config
    ):
        """
        Given: File on source FTP
        When: Send Kafka message (container consumer processes it)
        Then: File appears on destination FTP
        """
        from etl.transfer.ftp import FTPTransfer

        test_content = f"E2E test content - {time.time()}"
        timestamp = int(time.time())
        src_path = f"/testserver01/e2e_test_{timestamp}.txt"
        dst_path = f"/testserver02/e2e_received_{timestamp}.txt"

        # 1. Create and upload test file to source FTP
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(test_content)
            local_path = f.name

        try:
            with FTPTransfer(source_config, passive_mode=True) as src:
                src.upload(local_path, src_path)

            # 2. Send Kafka message (container consumer will process)
            job = {
                "job_id": f"e2e-test-{timestamp}",
                "source": {
                    "hostname": "SRC_FTP_SERVER1",
                    "path": src_path
                },
                "destination": {
                    "hostname": "DST_FTP_SERVER1",
                    "path": dst_path
                }
            }
            kafka_producer.send("test-transfer", value=json.dumps(job))
            kafka_producer.flush()

            # 3. Wait for container consumer to process (polling)
            downloaded_content = None

            def check_file_transferred():
                nonlocal downloaded_content
                try:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp_download = tmp.name
                    with FTPTransfer(dest_config, passive_mode=True) as dst:
                        dst.download(dst_path, tmp_download)
                    with open(tmp_download, "r") as f:
                        downloaded_content = f.read()
                    os.unlink(tmp_download)
                    return True
                except Exception:
                    return False

            assert wait_for_condition(check_file_transferred, timeout=E2E_TRANSFER_TIMEOUT), \
                f"File not transferred within {E2E_TRANSFER_TIMEOUT}s timeout"

            # 4. Verify downloaded content
            assert downloaded_content == test_content

        finally:
            os.unlink(local_path)


class TestDLQPipeline:
    """Test DLQ handling end-to-end."""

    def test_failed_transfer_goes_to_dlq(self, kafka_producer, kafka_bootstrap_servers):
        """
        Given: Message with non-existent source file
        When: Container consumer processes message
        Then: Message appears in DLQ topic
        """
        from kafka import KafkaConsumer

        timestamp = int(time.time())

        # Send job with non-existent file
        job = {
            "job_id": f"dlq-test-{timestamp}",
            "source": {
                "hostname": "SRC_FTP_SERVER1",
                "path": f"/testserver01/nonexistent_file_{timestamp}.txt"
            },
            "destination": {
                "hostname": "DST_FTP_SERVER1",
                "path": "/testserver02/out.txt"
            }
        }
        kafka_producer.send("test-transfer", value=json.dumps(job))
        kafka_producer.flush()

        # Poll DLQ for message with our job_id
        dlq_consumer = KafkaConsumer(
            "test-transfer-dlq",
            bootstrap_servers=[kafka_bootstrap_servers],
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,  # Short timeout for polling
            value_deserializer=lambda m: m.decode("utf-8"),
        )

        found_message = False
        dlq_messages = []

        def check_dlq_message():
            nonlocal found_message, dlq_messages
            for msg in dlq_consumer:
                dlq_messages.append(msg.value)
                if f"dlq-test-{timestamp}" in msg.value:
                    found_message = True
                    return True
            return False

        wait_for_condition(check_dlq_message)
        dlq_consumer.close()

        # Verify our message was found in DLQ
        assert found_message, \
            f"Expected job_id dlq-test-{timestamp} in DLQ within {E2E_TIMEOUT}s. Got: {dlq_messages}"

    def test_invalid_json_goes_to_dlq(self, kafka_producer, kafka_bootstrap_servers):
        """
        Given: Invalid JSON message
        When: Container consumer processes message
        Then: Raw message appears in DLQ topic
        """
        from kafka import KafkaConsumer

        timestamp = int(time.time())
        invalid_message = f"invalid json message - {timestamp}"

        # Send invalid JSON
        kafka_producer.send("test-transfer", value=invalid_message)
        kafka_producer.flush()

        # Poll DLQ for our invalid message
        dlq_consumer = KafkaConsumer(
            "test-transfer-dlq",
            bootstrap_servers=[kafka_bootstrap_servers],
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,  # Short timeout for polling
            value_deserializer=lambda m: m.decode("utf-8"),
        )

        found_message = False
        dlq_messages = []

        def check_dlq_message():
            nonlocal found_message, dlq_messages
            for msg in dlq_consumer:
                dlq_messages.append(msg.value)
                if str(timestamp) in msg.value:
                    found_message = True
                    return True
            return False

        wait_for_condition(check_dlq_message)
        dlq_consumer.close()

        # Verify our invalid message is in DLQ
        assert found_message, \
            f"Expected timestamp {timestamp} in DLQ within {E2E_TIMEOUT}s. Got: {dlq_messages}"
