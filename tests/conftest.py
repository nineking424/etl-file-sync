"""Pytest configuration and fixtures."""

import os
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
