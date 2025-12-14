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


class TestFTPToLocalTransfer:
    """Test FTP to Local file transfer."""

    def test_ftp_to_local_transfer(
        self, kafka_producer, source_config, shared_test_dir
    ):
        """
        Given: File on source FTP
        When: Send Kafka message (source=FTP, dest=Local)
        Then: File appears in shared local directory
        """
        from etl.transfer.ftp import FTPTransfer

        test_content = f"FTP to Local test - {time.time()}"
        timestamp = int(time.time())
        src_path = f"/testserver01/ftp_to_local_{timestamp}.txt"
        dst_path = f"/shared/destination/from_ftp_{timestamp}.txt"

        # 1. Upload to source FTP
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write(test_content)
            local_path = f.name

        try:
            with FTPTransfer(source_config, passive_mode=True) as src:
                src.upload(local_path, src_path)

            # 2. Send Kafka message
            job = {
                "job_id": f"ftp-to-local-{timestamp}",
                "source": {"hostname": "SRC_FTP_SERVER1", "path": src_path},
                "destination": {"hostname": "LOCAL_SERVER1", "path": dst_path}
            }
            kafka_producer.send("test-transfer", value=json.dumps(job))
            kafka_producer.flush()

            # 3. Verify file appears locally
            local_dst = shared_test_dir["destination"] / f"from_ftp_{timestamp}.txt"

            def check_local_file():
                return local_dst.exists()

            assert wait_for_condition(check_local_file, timeout=E2E_TRANSFER_TIMEOUT), \
                f"File not transferred within {E2E_TRANSFER_TIMEOUT}s timeout"
            assert local_dst.read_text() == test_content

        finally:
            os.unlink(local_path)


class TestLocalToFTPTransfer:
    """Test Local to FTP file transfer."""

    def test_local_to_ftp_transfer(
        self, kafka_producer, dest_config, shared_test_dir
    ):
        """
        Given: File in shared local directory
        When: Send Kafka message (source=Local, dest=FTP)
        Then: File appears on destination FTP
        """
        from etl.transfer.ftp import FTPTransfer

        test_content = f"Local to FTP test - {time.time()}"
        timestamp = int(time.time())
        src_path = f"/shared/source/to_ftp_{timestamp}.txt"
        dst_path = f"/testserver02/from_local_{timestamp}.txt"

        # 1. Create local file in shared directory
        local_src = shared_test_dir["source"] / f"to_ftp_{timestamp}.txt"
        local_src.write_text(test_content)

        # Wait for Docker Desktop volume sync
        time.sleep(2)

        # 2. Send Kafka message
        job = {
            "job_id": f"local-to-ftp-{timestamp}",
            "source": {"hostname": "LOCAL_SERVER1", "path": src_path},
            "destination": {"hostname": "DST_FTP_SERVER1", "path": dst_path}
        }
        kafka_producer.send("test-transfer", value=json.dumps(job))
        kafka_producer.flush()

        # 3. Verify file on FTP
        downloaded_content = None

        def check_ftp_file():
            nonlocal downloaded_content
            try:
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    tmp_path = tmp.name
                with FTPTransfer(dest_config, passive_mode=True) as dst:
                    dst.download(dst_path, tmp_path)
                with open(tmp_path, "r") as f:
                    downloaded_content = f.read()
                os.unlink(tmp_path)
                return True
            except Exception:
                return False

        assert wait_for_condition(check_ftp_file, timeout=E2E_TRANSFER_TIMEOUT), \
            f"File not transferred within {E2E_TRANSFER_TIMEOUT}s timeout"
        assert downloaded_content == test_content


class TestLocalToLocalTransfer:
    """Test Local to Local file transfer."""

    def test_local_to_local_transfer(
        self, kafka_producer, shared_test_dir
    ):
        """
        Given: File in shared source directory
        When: Send Kafka message (source=Local, dest=Local)
        Then: File appears in shared destination directory
        """
        test_content = f"Local to Local test - {time.time()}"
        timestamp = int(time.time())
        src_path = f"/shared/source/local_src_{timestamp}.txt"
        dst_path = f"/shared/destination/local_dst_{timestamp}.txt"

        # 1. Create source file
        local_src = shared_test_dir["source"] / f"local_src_{timestamp}.txt"
        local_src.write_text(test_content)

        # Wait for Docker Desktop volume sync
        time.sleep(2)

        # 2. Send Kafka message
        job = {
            "job_id": f"local-to-local-{timestamp}",
            "source": {"hostname": "LOCAL_SERVER1", "path": src_path},
            "destination": {"hostname": "LOCAL_SERVER1", "path": dst_path}
        }
        kafka_producer.send("test-transfer", value=json.dumps(job))
        kafka_producer.flush()

        # 3. Verify destination file
        local_dst = shared_test_dir["destination"] / f"local_dst_{timestamp}.txt"

        def check_local_file():
            return local_dst.exists()

        assert wait_for_condition(check_local_file, timeout=E2E_TRANSFER_TIMEOUT), \
            f"File not transferred within {E2E_TRANSFER_TIMEOUT}s timeout"
        assert local_dst.read_text() == test_content


@pytest.mark.slow
class TestBulkTransfer:
    """Test bulk file transfers."""

    def test_ftp_to_ftp_bulk_transfer(
        self, kafka_producer, source_config, dest_config
    ):
        """
        Given: 1000 files on source FTP
        When: Send 1000 Kafka messages
        Then: All files appear on destination FTP
        """
        from etl.transfer.ftp import FTPTransfer

        FILE_COUNT = 1000
        BULK_TIMEOUT = 300.0  # 5 minutes for bulk transfer
        timestamp = int(time.time())

        # Track files
        files = []

        # 1. Generate and upload files to source FTP
        with FTPTransfer(source_config, passive_mode=True) as src:
            for i in range(FILE_COUNT):
                content = f"Bulk file {i} - {timestamp}"
                src_path = f"/testserver01/bulk_{timestamp}_{i:04d}.txt"
                dst_path = f"/testserver02/bulk_{timestamp}_{i:04d}.txt"

                with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                    f.write(content)
                    tmp_path = f.name

                src.upload(tmp_path, src_path)
                os.unlink(tmp_path)

                files.append({
                    "src": src_path,
                    "dst": dst_path,
                    "content": content
                })

        # 2. Send Kafka messages
        for i, file_info in enumerate(files):
            job = {
                "job_id": f"bulk-{timestamp}-{i:04d}",
                "source": {"hostname": "SRC_FTP_SERVER1", "path": file_info["src"]},
                "destination": {"hostname": "DST_FTP_SERVER1", "path": file_info["dst"]}
            }
            kafka_producer.send("test-transfer", value=json.dumps(job))
        kafka_producer.flush()

        # 3. Wait and verify transfers
        success_count = 0
        fail_count = 0
        start_time = time.time()

        def check_all_transferred():
            nonlocal success_count, fail_count
            success_count = 0
            fail_count = 0

            with FTPTransfer(dest_config, passive_mode=True) as dst:
                for file_info in files:
                    try:
                        with tempfile.NamedTemporaryFile(delete=False) as tmp:
                            tmp_path = tmp.name
                        dst.download(file_info["dst"], tmp_path)
                        with open(tmp_path, "r") as f:
                            if f.read() == file_info["content"]:
                                success_count += 1
                            else:
                                fail_count += 1
                        os.unlink(tmp_path)
                    except Exception:
                        fail_count += 1

            return success_count == FILE_COUNT

        result = wait_for_condition(
            check_all_transferred,
            timeout=BULK_TIMEOUT,
            interval=5.0  # Check every 5 seconds for bulk
        )

        elapsed = time.time() - start_time

        # Report results
        print(f"\n=== Bulk Transfer Results ===")
        print(f"Total files: {FILE_COUNT}")
        print(f"Success: {success_count}")
        print(f"Failed: {fail_count}")
        print(f"Elapsed: {elapsed:.2f}s")
        print(f"Rate: {success_count/elapsed:.2f} files/sec")

        assert result, f"Bulk transfer incomplete: {success_count}/{FILE_COUNT} succeeded"
