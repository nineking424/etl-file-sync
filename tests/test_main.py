"""Tests for main.py CLI and signal handling."""

import signal
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock

from etl import main as main_module
from etl.main import parse_args, signal_handler, main


class TestArgumentParsing:
    """Test CLI argument parsing."""

    def test_default_arguments(self):
        """Test default argument values."""
        with patch.object(sys, "argv", ["main.py"]):
            args = parse_args()

            assert args.topic == "file-transfer-jobs"
            assert args.group_id == "etl-worker-group"
            assert args.bootstrap_servers == "localhost:9092"
            assert args.env_file is None
            assert args.verbose is False

    def test_positional_topic(self):
        """Test positional topic argument."""
        with patch.object(sys, "argv", ["main.py", "my-topic"]):
            args = parse_args()

            assert args.topic == "my-topic"
            assert args.group_id == "etl-worker-group"

    def test_positional_all(self):
        """Test all positional arguments."""
        with patch.object(sys, "argv", ["main.py", "my-topic", "my-group", "kafka:9092"]):
            args = parse_args()

            assert args.topic == "my-topic"
            assert args.group_id == "my-group"
            assert args.bootstrap_servers == "kafka:9092"

    def test_optional_overrides(self):
        """Test optional arguments override positional."""
        with patch.object(sys, "argv", [
            "main.py",
            "pos-topic",
            "--topic", "opt-topic",
            "--group-id", "opt-group",
            "--bootstrap-servers", "opt-kafka:9092",
        ]):
            args = parse_args()

            # Optional should be used over positional in main()
            assert args.topic == "pos-topic"  # Positional
            assert args.topic_opt == "opt-topic"  # Optional
            assert args.group_id_opt == "opt-group"

    def test_env_file_option(self):
        """Test --env-file option."""
        with patch.object(sys, "argv", ["main.py", "--env-file", "/path/to/.env"]):
            args = parse_args()

            assert args.env_file == "/path/to/.env"

    def test_verbose_flag(self):
        """Test -v/--verbose flag."""
        with patch.object(sys, "argv", ["main.py", "-v"]):
            args = parse_args()
            assert args.verbose is True

        with patch.object(sys, "argv", ["main.py", "--verbose"]):
            args = parse_args()
            assert args.verbose is True

    def test_all_options_combined(self):
        """Test all options combined."""
        with patch.object(sys, "argv", [
            "main.py",
            "--topic", "test-topic",
            "--group-id", "test-group",
            "--bootstrap-servers", "kafka:9092",
            "--env-file", "/etc/etl/.env",
            "-v",
        ]):
            args = parse_args()

            assert args.topic_opt == "test-topic"
            assert args.group_id_opt == "test-group"
            assert args.bootstrap_servers_opt == "kafka:9092"
            assert args.env_file == "/etc/etl/.env"
            assert args.verbose is True


class TestSignalHandling:
    """Test signal handler functionality."""

    def test_sigint_stops_consumer(self):
        """Test SIGINT calls consumer.stop()."""
        mock_consumer = Mock()
        main_module._consumer = mock_consumer

        with pytest.raises(SystemExit) as exc_info:
            signal_handler(signal.SIGINT, None)

        mock_consumer.stop.assert_called_once()
        assert exc_info.value.code == 0

        # Cleanup
        main_module._consumer = None

    def test_sigterm_stops_consumer(self):
        """Test SIGTERM calls consumer.stop()."""
        mock_consumer = Mock()
        main_module._consumer = mock_consumer

        with pytest.raises(SystemExit) as exc_info:
            signal_handler(signal.SIGTERM, None)

        mock_consumer.stop.assert_called_once()
        assert exc_info.value.code == 0

        # Cleanup
        main_module._consumer = None

    def test_signal_when_consumer_none(self):
        """Test signal handler when consumer is None."""
        main_module._consumer = None

        with pytest.raises(SystemExit) as exc_info:
            signal_handler(signal.SIGINT, None)

        # Should exit cleanly without error
        assert exc_info.value.code == 0


class TestMainFunction:
    """Test main() function."""

    def test_main_creates_consumer(self):
        """Test main() creates and starts consumer."""
        with patch.object(sys, "argv", ["main.py"]):
            with patch("etl.main.FileTransferConsumer") as MockConsumer:
                mock_instance = MagicMock()
                MockConsumer.return_value = mock_instance
                mock_instance.start.side_effect = KeyboardInterrupt()

                main()

                MockConsumer.assert_called_once_with(
                    topic="file-transfer-jobs",
                    group_id="etl-worker-group",
                    bootstrap_servers="localhost:9092",
                )
                mock_instance.start.assert_called_once()
                mock_instance.stop.assert_called_once()

    def test_main_uses_optional_over_positional(self):
        """Test main() prefers optional arguments over positional."""
        with patch.object(sys, "argv", [
            "main.py",
            "pos-topic",
            "--topic", "opt-topic",
        ]):
            with patch("etl.main.FileTransferConsumer") as MockConsumer:
                mock_instance = MagicMock()
                MockConsumer.return_value = mock_instance
                mock_instance.start.side_effect = KeyboardInterrupt()

                main()

                # Optional topic should be used
                call_kwargs = MockConsumer.call_args[1]
                assert call_kwargs["topic"] == "opt-topic"

    def test_main_loads_env_file(self):
        """Test main() loads env file when specified."""
        with patch.object(sys, "argv", ["main.py", "--env-file", "/path/.env"]):
            with patch("etl.main.FileTransferConsumer") as MockConsumer:
                with patch("etl.config.get_config") as mock_get_config:
                    mock_instance = MagicMock()
                    MockConsumer.return_value = mock_instance
                    mock_instance.start.side_effect = KeyboardInterrupt()

                    main()

                    mock_get_config.assert_called_once_with("/path/.env")

    def test_main_verbose_sets_debug(self):
        """Test main() sets DEBUG level when verbose."""
        with patch.object(sys, "argv", ["main.py", "-v"]):
            with patch("etl.main.FileTransferConsumer") as MockConsumer:
                with patch("logging.getLogger") as mock_get_logger:
                    mock_root_logger = Mock()
                    mock_get_logger.return_value = mock_root_logger

                    mock_instance = MagicMock()
                    MockConsumer.return_value = mock_instance
                    mock_instance.start.side_effect = KeyboardInterrupt()

                    main()

                    # Verify setLevel was called (with DEBUG=10)
                    mock_root_logger.setLevel.assert_called()

    def test_main_stops_consumer_on_exception(self):
        """Test main() stops consumer when exception occurs."""
        with patch.object(sys, "argv", ["main.py"]):
            with patch("etl.main.FileTransferConsumer") as MockConsumer:
                mock_instance = MagicMock()
                MockConsumer.return_value = mock_instance
                mock_instance.start.side_effect = RuntimeError("Test error")

                with pytest.raises(RuntimeError):
                    main()

                mock_instance.stop.assert_called_once()
