"""Pytest configuration and fixtures."""

import os
import shutil
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
    host = os.getenv("SRC_FTP_SERVER1_HOST", "localhost")
    port = int(os.getenv("SRC_FTP_SERVER1_PORT", "21"))
    try:
        sock = socket.create_connection((host, port), timeout=2)
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
# Note: Kafka topics are automatically created by the container (docker/entrypoint.sh)
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


# Local transfer fixtures for E2E tests
# Note: Use a path accessible by Docker Desktop (macOS shares /Users by default)
SHARED_TEST_DIR = str(Path(__file__).parent.parent / ".etl-test-shared")


@pytest.fixture
def shared_test_dir():
    """Create shared directory for container-host file sharing.

    This directory is mounted as a volume in docker-compose.test.yml:
    Host: .etl-test-shared -> Container: /shared

    Note: We preserve the base directory to maintain the Docker bind mount.
    Only the contents of subdirectories are cleaned between tests.
    """
    base_dir = Path(SHARED_TEST_DIR)
    src_dir = base_dir / "source"
    dst_dir = base_dir / "destination"

    # Ensure base directory exists (preserve bind mount)
    base_dir.mkdir(parents=True, exist_ok=True)

    # Clean subdirectory contents (not the directories themselves)
    for d in [src_dir, dst_dir]:
        if d.exists():
            # Remove all files in the directory
            for item in d.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
        else:
            d.mkdir(parents=True)

    yield {"source": src_dir, "destination": dst_dir}

    # Don't delete base directory - preserve bind mount for container
    # Only clean contents after test
    for d in [src_dir, dst_dir]:
        if d.exists():
            for item in d.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)


@pytest.fixture
def local_config(test_env_file):
    """Get local transfer config."""
    from etl.config import ConfigLoader

    config = ConfigLoader(test_env_file)
    return config.get_server_config("LOCAL_SERVER1")


# =============================================================================
# Priority-Based Test Ordering
# =============================================================================

# Priority mapping: lower number = higher priority (runs first)
MARKER_PRIORITY = {
    "e2e": 1,
    "integration": 2,
    "unit": 3,
}

DEFAULT_PRIORITY = 99  # Tests without markers run last


def get_test_priority(item):
    """Get priority for a test item based on its markers."""
    for marker_name, priority in MARKER_PRIORITY.items():
        if marker_name in item.keywords:
            return priority
    return DEFAULT_PRIORITY


def pytest_collection_modifyitems(session, config, items):
    """
    Reorder tests by priority within a single pytest run.

    Priority Order:
    1. E2E tests (highest priority) - validate full system
    2. Integration tests - validate component interactions
    3. Unit tests (lowest priority) - validate individual units

    This ensures that when running all tests together,
    higher priority tests execute first.
    """
    # Sort items by priority (stable sort preserves order within same priority)
    items.sort(key=get_test_priority)


# =============================================================================
# Session-Level Result Tracking (for conditional execution)
# =============================================================================


class PriorityTestResults:
    """Track test results by priority level."""

    def __init__(self):
        self.results = {
            "e2e": {"passed": 0, "failed": 0, "skipped": 0},
            "integration": {"passed": 0, "failed": 0, "skipped": 0},
            "unit": {"passed": 0, "failed": 0, "skipped": 0},
        }

    def record(self, item, outcome):
        """Record a test result."""
        for marker_name in self.results.keys():
            if marker_name in item.keywords:
                self.results[marker_name][outcome] += 1
                break

    def has_failures(self, marker_name):
        """Check if a priority level has failures."""
        return self.results.get(marker_name, {}).get("failed", 0) > 0

    def all_passed(self, marker_name):
        """Check if all tests in a priority level passed."""
        result = self.results.get(marker_name, {})
        return result.get("failed", 0) == 0 and result.get("passed", 0) > 0


# Global results tracker
_priority_results = PriorityTestResults()


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Track test results by priority level."""
    outcome = yield
    report = outcome.get_result()

    if report.when == "call":
        if report.passed:
            _priority_results.record(item, "passed")
        elif report.failed:
            _priority_results.record(item, "failed")
        elif report.skipped:
            _priority_results.record(item, "skipped")


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Add priority-based summary to test output."""
    terminalreporter.write_sep("=", "Priority-Based Test Summary")

    for marker_name in ["e2e", "integration", "unit"]:
        result = _priority_results.results[marker_name]
        total = result["passed"] + result["failed"] + result["skipped"]

        if total > 0:
            status = "PASSED" if result["failed"] == 0 else "FAILED"
            terminalreporter.write_line(
                f"  {marker_name.upper():12} - {status}: "
                f"{result['passed']} passed, {result['failed']} failed, "
                f"{result['skipped']} skipped"
            )
