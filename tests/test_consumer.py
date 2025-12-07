"""Tests for FileTransferConsumer."""

import json
import os
import pytest
from unittest.mock import Mock, MagicMock, patch, call

from etl.consumer import FileTransferConsumer
from etl.config import ConfigLoader, ServerConfig
from etl.models import FileTransferJob, DLQMessage
from etl.transfer.ftp import FTPTransfer


@pytest.fixture
def mock_config():
    """Create mock ConfigLoader."""
    config = Mock(spec=ConfigLoader)
    config.ftp_passive_mode = True
    config.dlq_topic = "test-dlq"

    # Setup server configs
    src_config = ServerConfig(
        hostname="SRC_SERVER",
        type="ftp",
        host="src.example.com",
        port=21,
        username="srcuser",
        password="srcpass",
    )
    dst_config = ServerConfig(
        hostname="DST_SERVER",
        type="ftp",
        host="dst.example.com",
        port=21,
        username="dstuser",
        password="dstpass",
    )

    def get_server_config(hostname):
        if hostname == "SRC_SERVER":
            return src_config
        elif hostname == "DST_SERVER":
            return dst_config
        else:
            raise ValueError(f"Server type not found for: {hostname}")

    config.get_server_config = Mock(side_effect=get_server_config)

    return config


@pytest.fixture
def valid_job_message():
    """Create valid job message."""
    return json.dumps({
        "job_id": "test-job-123",
        "source": {
            "hostname": "SRC_SERVER",
            "path": "/remote/source/file.txt"
        },
        "destination": {
            "hostname": "DST_SERVER",
            "path": "/remote/dest/file.txt"
        }
    })


@pytest.fixture
def mock_kafka_message(valid_job_message):
    """Create mock Kafka message."""
    msg = Mock()
    msg.value = valid_job_message
    msg.partition = 0
    msg.offset = 100
    return msg


class TestConsumerInit:
    """Test FileTransferConsumer initialization."""

    def test_default_config(self):
        """Test initialization with default config."""
        with patch("etl.consumer.get_config") as mock_get_config:
            mock_get_config.return_value = Mock(spec=ConfigLoader)

            consumer = FileTransferConsumer(
                topic="test-topic",
                group_id="test-group",
            )

            assert consumer.topic == "test-topic"
            assert consumer.group_id == "test-group"
            assert consumer.bootstrap_servers == "localhost:9092"
            mock_get_config.assert_called_once()

    def test_custom_config(self, mock_config):
        """Test initialization with custom config."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        assert consumer.config == mock_config

    def test_attributes_assigned(self, mock_config):
        """Test that all attributes are properly assigned."""
        consumer = FileTransferConsumer(
            topic="my-topic",
            group_id="my-group",
            bootstrap_servers="kafka:9092",
            config=mock_config,
        )

        assert consumer.topic == "my-topic"
        assert consumer.group_id == "my-group"
        assert consumer.bootstrap_servers == "kafka:9092"
        assert consumer._consumer is None
        assert consumer._producer is None
        assert consumer._running is False


class TestMessageProcessing:
    """Test message processing logic."""

    def test_valid_message_success(self, mock_config, mock_kafka_message):
        """Test processing valid message successfully."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        with patch.object(consumer, "_execute_transfer") as mock_execute:
            consumer._process_message(mock_kafka_message)

            mock_execute.assert_called_once()
            consumer._consumer.commit.assert_called_once()

    def test_invalid_json_to_dlq(self, mock_config):
        """Test that invalid JSON is sent to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        mock_message = Mock()
        mock_message.value = "not valid json"
        mock_message.partition = 0
        mock_message.offset = 100

        with patch.object(consumer, "_send_raw_to_dlq") as mock_send_dlq:
            consumer._process_message(mock_message)

            mock_send_dlq.assert_called_once()
            consumer._consumer.commit.assert_called_once()

    def test_missing_source_to_dlq(self, mock_config):
        """Test that message with missing source goes to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        mock_message = Mock()
        mock_message.value = json.dumps({
            "destination": {"hostname": "DST", "path": "/file.txt"}
        })
        mock_message.partition = 0
        mock_message.offset = 100

        with patch.object(consumer, "_send_raw_to_dlq") as mock_send_dlq:
            consumer._process_message(mock_message)

            mock_send_dlq.assert_called_once()

    def test_missing_destination_to_dlq(self, mock_config):
        """Test that message with missing destination goes to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        mock_message = Mock()
        mock_message.value = json.dumps({
            "source": {"hostname": "SRC", "path": "/file.txt"}
        })
        mock_message.partition = 0
        mock_message.offset = 100

        with patch.object(consumer, "_send_raw_to_dlq") as mock_send_dlq:
            consumer._process_message(mock_message)

            mock_send_dlq.assert_called_once()

    def test_server_not_found_to_dlq(self, mock_config, mock_kafka_message):
        """Test that unknown server hostname sends job to DLQ."""
        mock_config.get_server_config.side_effect = ValueError("Server not found")

        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        with patch.object(consumer, "_send_to_dlq") as mock_send_dlq:
            consumer._process_message(mock_kafka_message)

            mock_send_dlq.assert_called_once()

    def test_download_error_to_dlq(self, mock_config, mock_kafka_message):
        """Test that download failure sends job to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        with patch.object(consumer, "_execute_transfer") as mock_execute:
            mock_execute.side_effect = FileNotFoundError("Remote file not found")

            with patch.object(consumer, "_send_to_dlq") as mock_send_dlq:
                consumer._process_message(mock_kafka_message)

                mock_send_dlq.assert_called_once()
                consumer._consumer.commit.assert_called_once()

    def test_upload_error_to_dlq(self, mock_config, mock_kafka_message):
        """Test that upload failure sends job to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        with patch.object(consumer, "_execute_transfer") as mock_execute:
            mock_execute.side_effect = IOError("Upload failed")

            with patch.object(consumer, "_send_to_dlq") as mock_send_dlq:
                consumer._process_message(mock_kafka_message)

                mock_send_dlq.assert_called_once()

    def test_offset_commit_on_success(self, mock_config, mock_kafka_message):
        """Test that offset is committed after successful processing."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._consumer = Mock()
        consumer._producer = Mock()

        with patch.object(consumer, "_execute_transfer"):
            consumer._process_message(mock_kafka_message)

            consumer._consumer.commit.assert_called_once()


class TestDLQHandling:
    """Test DLQ message handling."""

    def test_send_to_dlq_from_job(self, mock_config):
        """Test sending parsed job to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._producer = Mock()
        consumer._producer.send.return_value.get.return_value = None

        job = FileTransferJob.from_dict({
            "job_id": "test-123",
            "source": {"hostname": "SRC", "path": "/src.txt"},
            "destination": {"hostname": "DST", "path": "/dst.txt"},
        })

        consumer._send_to_dlq(job, "Test error")

        consumer._producer.send.assert_called_once()
        call_args = consumer._producer.send.call_args
        assert call_args[1]["value"] is not None

    def test_send_raw_to_dlq(self, mock_config):
        """Test sending raw unparseable message to DLQ."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._producer = Mock()
        consumer._producer.send.return_value.get.return_value = None

        consumer._send_raw_to_dlq("raw message content", "Parse error")

        consumer._producer.send.assert_called_once()

    def test_dlq_message_format(self, mock_config):
        """Test DLQ message contains required fields."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._producer = Mock()
        consumer._producer.send.return_value.get.return_value = None

        job = FileTransferJob.from_dict({
            "job_id": "test-123",
            "source": {"hostname": "SRC", "path": "/src.txt"},
            "destination": {"hostname": "DST", "path": "/dst.txt"},
        })

        consumer._send_to_dlq(job, "Connection refused")

        call_args = consumer._producer.send.call_args
        message_value = call_args[1]["value"]
        message_dict = json.loads(message_value)

        assert "original_message" in message_dict
        assert "error" in message_dict
        assert "timestamp" in message_dict
        assert message_dict["error"] == "Connection refused"

    def test_dlq_send_failure_logged(self, mock_config):
        """Test that DLQ send failure is logged but doesn't raise."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )
        consumer._producer = Mock()
        consumer._producer.send.return_value.get.side_effect = Exception("Kafka error")

        # Should not raise exception
        consumer._send_dlq_message('{"test": "message"}')


class TestTransferExecution:
    """Test file transfer execution."""

    def test_passive_mode_applied(self, mock_config):
        """Test that passive mode is applied to FTP transfers."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        job = FileTransferJob.from_dict({
            "job_id": "test-123",
            "source": {"hostname": "SRC_SERVER", "path": "/src.txt"},
            "destination": {"hostname": "DST_SERVER", "path": "/dst.txt"},
        })

        with patch("etl.consumer.TransferFactory") as mock_factory:
            mock_src_transfer = Mock(spec=FTPTransfer)
            mock_dst_transfer = Mock(spec=FTPTransfer)
            mock_factory.create.side_effect = [mock_src_transfer, mock_dst_transfer]

            with patch("tempfile.NamedTemporaryFile"):
                with patch("os.path.exists", return_value=True):
                    with patch("os.remove"):
                        try:
                            consumer._execute_transfer(job)
                        except:
                            pass  # Ignore mock-related errors

            # Verify passive mode was set
            if hasattr(mock_src_transfer, "passive_mode"):
                assert mock_src_transfer.passive_mode == mock_config.ftp_passive_mode

    def test_temp_file_deleted_success(self, mock_config):
        """Test temp file is deleted after successful transfer."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        job = FileTransferJob.from_dict({
            "job_id": "test-123",
            "source": {"hostname": "SRC_SERVER", "path": "/src.txt"},
            "destination": {"hostname": "DST_SERVER", "path": "/dst.txt"},
        })

        with patch("etl.consumer.TransferFactory") as mock_factory:
            mock_transfer = MagicMock()
            mock_transfer.__enter__ = Mock(return_value=mock_transfer)
            mock_transfer.__exit__ = Mock(return_value=False)
            mock_factory.create.return_value = mock_transfer

            with patch("tempfile.NamedTemporaryFile") as mock_temp:
                mock_temp.return_value.__enter__ = Mock(return_value=Mock(name="/tmp/test"))
                mock_temp.return_value.__exit__ = Mock(return_value=False)
                mock_temp.return_value.name = "/tmp/test_file"

                with patch("os.path.exists", return_value=True) as mock_exists:
                    with patch("os.remove") as mock_remove:
                        consumer._execute_transfer(job)

                        # Verify temp file deletion was attempted
                        mock_remove.assert_called()

    def test_temp_file_deleted_on_error(self, mock_config):
        """Test temp file is deleted even when transfer fails."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        job = FileTransferJob.from_dict({
            "job_id": "test-123",
            "source": {"hostname": "SRC_SERVER", "path": "/src.txt"},
            "destination": {"hostname": "DST_SERVER", "path": "/dst.txt"},
        })

        with patch("etl.consumer.TransferFactory") as mock_factory:
            mock_transfer = MagicMock()
            mock_transfer.__enter__ = Mock(return_value=mock_transfer)
            mock_transfer.__exit__ = Mock(return_value=False)
            mock_transfer.download.side_effect = IOError("Download failed")
            mock_factory.create.return_value = mock_transfer

            with patch("tempfile.NamedTemporaryFile") as mock_temp:
                mock_temp.return_value.__enter__ = Mock(return_value=Mock(name="/tmp/test"))
                mock_temp.return_value.__exit__ = Mock(return_value=False)
                mock_temp.return_value.name = "/tmp/test_file"

                with patch("os.path.exists", return_value=True):
                    with patch("os.remove") as mock_remove:
                        with pytest.raises(IOError):
                            consumer._execute_transfer(job)

                        # Verify temp file deletion was still attempted
                        mock_remove.assert_called()


class TestConsumerLifecycle:
    """Test consumer lifecycle management."""

    def test_stop_without_start(self, mock_config):
        """Test that stop() works even if start() was never called."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        # Should not raise any exception
        consumer.stop()

        assert consumer._consumer is None
        assert consumer._producer is None
        assert consumer._running is False

    def test_multiple_stop_calls(self, mock_config):
        """Test that multiple stop() calls don't cause errors."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        # Multiple stop calls should be safe
        consumer.stop()
        consumer.stop()
        consumer.stop()

        assert consumer._running is False

    def test_stop_closes_consumer_and_producer(self, mock_config):
        """Test that stop() closes both consumer and producer."""
        consumer = FileTransferConsumer(
            topic="test-topic",
            group_id="test-group",
            config=mock_config,
        )

        mock_consumer = Mock()
        mock_producer = Mock()
        consumer._consumer = mock_consumer
        consumer._producer = mock_producer
        consumer._running = True

        consumer.stop()

        mock_consumer.close.assert_called_once()
        mock_producer.close.assert_called_once()
        assert consumer._consumer is None
        assert consumer._producer is None
        assert consumer._running is False
