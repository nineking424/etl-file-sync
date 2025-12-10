"""Kafka consumer for file transfer jobs."""

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer

from .config import ConfigLoader, get_config
from .models import DLQMessage, FileTransferJob
from .transfer import TransferFactory
from .transfer.ftp import FTPTransfer  # noqa: F401 - Register FTP handler

logger = logging.getLogger(__name__)


class FileTransferConsumer:
    """Kafka consumer that processes file transfer jobs."""

    def __init__(
        self,
        topic: str,
        group_id: str,
        bootstrap_servers: str = "localhost:9092",
        config: Optional[ConfigLoader] = None,
    ):
        """Initialize file transfer consumer.

        Args:
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            bootstrap_servers: Kafka bootstrap servers
            config: Configuration loader (uses global if not provided)
        """
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.config = config or get_config()

        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None
        self._running = False

    def start(self) -> None:
        """Start consuming messages."""
        logger.info(f"Starting consumer for topic: {self.topic}")
        logger.info(f"Group ID: {self.group_id}")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")

        self._consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: m.decode("utf-8"),
        )

        # Producer for DLQ
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            value_serializer=lambda m: m.encode("utf-8"),
        )

        self._running = True
        logger.info("Consumer started, waiting for messages...")

        try:
            self._consume_loop()
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False

        if self._consumer:
            self._consumer.close()
            self._consumer = None

        if self._producer:
            self._producer.close()
            self._producer = None

        logger.info("Consumer stopped")

    def _consume_loop(self) -> None:
        """Main consume loop."""
        while self._running:
            # Poll for messages
            message_batch = self._consumer.poll(timeout_ms=1000)

            for topic_partition, messages in message_batch.items():
                for message in messages:
                    self._process_message(message)

    def _process_message(self, message) -> None:
        """Process a single message.

        Args:
            message: Kafka message
        """
        job = None
        try:
            logger.info(
                f"Processing message: partition={message.partition}, "
                f"offset={message.offset}"
            )

            # Parse job from message
            job = FileTransferJob.from_json(message.value)
            logger.info(f"Job ID: {job.job_id}")
            logger.info(f"Source: {job.source.hostname}:{job.source.path}")
            logger.info(f"Destination: {job.destination.hostname}:{job.destination.path}")

            # Execute file transfer
            self._execute_transfer(job)

            # Commit offset on success
            self._consumer.commit()
            logger.info(f"Job {job.job_id} completed successfully")

        except Exception as e:
            logger.error(f"Job failed: {e}")

            # Send to DLQ
            if job:
                self._send_to_dlq(job, str(e))
            else:
                # Failed to parse job - send raw message to DLQ
                self._send_raw_to_dlq(message.value, str(e))

            # Commit offset even on failure (message goes to DLQ)
            self._consumer.commit()

    def _execute_transfer(self, job: FileTransferJob) -> None:
        """Execute file transfer for a job.

        Args:
            job: File transfer job

        Raises:
            Exception: If transfer fails
        """
        # Get server configs
        src_config = self.config.get_server_config(job.source.hostname)
        dst_config = self.config.get_server_config(job.destination.hostname)

        # Create temporary file for transfer
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Download from source
            logger.info(f"Downloading from {src_config.host}...")
            src_transfer = TransferFactory.create(src_config)

            # Set passive mode for FTP
            if isinstance(src_transfer, FTPTransfer):
                src_transfer.passive_mode = self.config.ftp_passive_mode

            with src_transfer:
                src_transfer.download(job.source.path, tmp_path)

            # Upload to destination
            logger.info(f"Uploading to {dst_config.host}...")
            dst_transfer = TransferFactory.create(dst_config)

            # Set passive mode for FTP
            if isinstance(dst_transfer, FTPTransfer):
                dst_transfer.passive_mode = self.config.ftp_passive_mode

            with dst_transfer:
                dst_transfer.upload(tmp_path, job.destination.path)

            logger.info("File transfer completed")

        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _send_to_dlq(self, job: FileTransferJob, error: str) -> None:
        """Send failed job to Dead Letter Queue.

        Args:
            job: Failed job
            error: Error message
        """
        dlq_message = DLQMessage.from_job(job, error)
        self._send_dlq_message(dlq_message.to_json())

    def _send_raw_to_dlq(self, raw_message: str, error: str) -> None:
        """Send unparseable message to Dead Letter Queue.

        Args:
            raw_message: Raw message that failed to parse
            error: Error message
        """
        dlq_message = DLQMessage(
            original_message={"raw": raw_message},
            error=f"Failed to parse message: {error}",
        )
        self._send_dlq_message(dlq_message.to_json())

    def _send_dlq_message(self, message: str) -> None:
        """Send message to DLQ topic.

        Args:
            message: JSON message to send
        """
        dlq_topic = self.config.get_dlq_topic(self.topic)
        logger.warning(f"Sending message to DLQ: {dlq_topic}")

        try:
            future = self._producer.send(dlq_topic, value=message)
            future.get(timeout=10)  # Wait for send to complete
            logger.info("Message sent to DLQ successfully")
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
