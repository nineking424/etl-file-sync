"""ETL File Sync - Main entry point."""

import argparse
import logging
import os
import signal
import sys

from .consumer import FileTransferConsumer

# Get worker ID from environment (set by supervisor for each process)
WORKER_ID = os.getenv("ETL_WORKER_ID", "worker-00")

# Configure logging with worker ID
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s - [{WORKER_ID}] - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

# Global consumer reference for signal handling
_consumer: FileTransferConsumer | None = None


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    if _consumer:
        _consumer.stop()
    sys.exit(0)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ETL File Sync - Kafka-based file transfer service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "topic",
        nargs="?",
        default="file-transfer-jobs",
        help="Kafka topic to consume from",
    )

    parser.add_argument(
        "group_id",
        nargs="?",
        default="etl-worker-group",
        help="Consumer group ID",
    )

    parser.add_argument(
        "bootstrap_servers",
        nargs="?",
        default="localhost:9092",
        help="Kafka bootstrap servers (comma-separated)",
    )

    parser.add_argument(
        "--topic",
        dest="topic_opt",
        help="Kafka topic to consume from (alternative to positional)",
    )

    parser.add_argument(
        "--group-id",
        dest="group_id_opt",
        help="Consumer group ID (alternative to positional)",
    )

    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers_opt",
        help="Kafka bootstrap servers (alternative to positional)",
    )

    parser.add_argument(
        "--env-file",
        help="Path to .env file",
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    global _consumer

    args = parse_args()

    # Use optional arguments if provided, otherwise use positional
    topic = args.topic_opt or args.topic
    group_id = args.group_id_opt or args.group_id
    bootstrap_servers = args.bootstrap_servers_opt or args.bootstrap_servers

    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load config if env file specified
    if args.env_file:
        from .config import get_config
        get_config(args.env_file)

    logger.info("=" * 50)
    logger.info("ETL File Sync Consumer Starting")
    logger.info("=" * 50)
    logger.info(f"Worker ID: {WORKER_ID}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Group ID: {group_id}")
    logger.info(f"Bootstrap Servers: {bootstrap_servers}")
    logger.info("=" * 50)

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start consumer
    _consumer = FileTransferConsumer(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
    )

    try:
        _consumer.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise
    finally:
        if _consumer:
            _consumer.stop()


if __name__ == "__main__":
    main()
