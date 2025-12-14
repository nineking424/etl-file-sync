"""Unit tests for FTP connection pool."""

import queue
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit


class TestPooledConnection:
    """Test PooledConnection wrapper class."""

    def test_context_manager_returns_ftp(self):
        """Test context manager returns FTP connection."""
        from etl.transfer.pool import PooledConnection

        mock_ftp = MagicMock()
        mock_pool = MagicMock()

        conn = PooledConnection(mock_ftp, mock_pool)

        with conn as ftp:
            assert ftp is mock_ftp

    def test_context_manager_returns_to_pool(self):
        """Test connection is returned to pool on exit."""
        from etl.transfer.pool import PooledConnection

        mock_ftp = MagicMock()
        mock_pool = MagicMock()

        conn = PooledConnection(mock_ftp, mock_pool)

        with conn:
            pass

        mock_pool.return_connection.assert_called_once_with(conn)

    def test_tracks_creation_and_usage_time(self):
        """Test connection tracks creation and usage time."""
        from etl.transfer.pool import PooledConnection

        mock_ftp = MagicMock()
        mock_pool = MagicMock()

        before = time.time()
        conn = PooledConnection(mock_ftp, mock_pool)
        after = time.time()

        assert before <= conn.created_at <= after
        assert before <= conn.last_used <= after


class TestFTPConnectionPool:
    """Test FTPConnectionPool class."""

    @pytest.fixture
    def mock_config(self):
        """Create mock server config."""
        config = MagicMock()
        config.host = "test.ftp.server"
        config.port = 21
        config.username = "testuser"
        config.password = "testpass"
        return config

    def test_pool_creates_connection_on_first_borrow(self, mock_config):
        """Test pool creates new connection on first borrow."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp = MagicMock()
            mock_ftp_class.return_value = mock_ftp

            pool = FTPConnectionPool(mock_config, pool_size=2)
            conn = pool.borrow()

            assert conn is not None
            assert conn.ftp is mock_ftp
            mock_ftp.connect.assert_called_once()
            mock_ftp.login.assert_called_once()

    def test_pool_reuses_returned_connection(self, mock_config):
        """Test pool reuses connection after return."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp = MagicMock()
            mock_ftp_class.return_value = mock_ftp
            # NOOP succeeds (connection is valid)
            mock_ftp.voidcmd.return_value = None

            pool = FTPConnectionPool(mock_config, pool_size=2)

            # Borrow and return
            conn1 = pool.borrow()
            pool.return_connection(conn1)

            # Borrow again - should get same connection
            conn2 = pool.borrow()

            assert conn2.ftp is conn1.ftp
            # Only one FTP instance created
            assert mock_ftp_class.call_count == 1

    def test_pool_creates_multiple_connections(self, mock_config):
        """Test pool can create multiple connections up to pool_size."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp_class.side_effect = [MagicMock(), MagicMock()]

            pool = FTPConnectionPool(mock_config, pool_size=2)

            conn1 = pool.borrow()
            conn2 = pool.borrow()

            assert conn1.ftp is not conn2.ftp
            assert mock_ftp_class.call_count == 2

    def test_pool_blocks_when_exhausted(self, mock_config):
        """Test pool blocks when all connections are borrowed."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp_class.return_value = MagicMock()

            pool = FTPConnectionPool(mock_config, pool_size=1, max_wait=0.5)

            # Borrow the only connection
            conn1 = pool.borrow()

            # Try to borrow another - should timeout
            with pytest.raises(TimeoutError):
                pool.borrow()

    def test_pool_unblocks_when_connection_returned(self, mock_config):
        """Test blocked borrow succeeds when connection is returned."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp = MagicMock()
            mock_ftp_class.return_value = mock_ftp
            mock_ftp.voidcmd.return_value = None

            pool = FTPConnectionPool(mock_config, pool_size=1, max_wait=2.0)

            # Borrow the only connection
            conn1 = pool.borrow()

            # Return in another thread after delay
            def return_later():
                time.sleep(0.3)
                pool.return_connection(conn1)

            thread = threading.Thread(target=return_later)
            thread.start()

            # This should succeed after connection is returned
            conn2 = pool.borrow()
            assert conn2 is not None

            thread.join()

    def test_pool_validates_connection_before_reuse(self, mock_config):
        """Test pool validates connection with NOOP before reuse."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp = MagicMock()
            mock_ftp_class.return_value = mock_ftp

            pool = FTPConnectionPool(mock_config, pool_size=2)

            conn = pool.borrow()
            pool.return_connection(conn)

            # Borrow again - should validate with NOOP
            mock_ftp.voidcmd.return_value = None
            pool.borrow()

            mock_ftp.voidcmd.assert_called_with("NOOP")

    def test_pool_replaces_invalid_connection(self, mock_config):
        """Test pool creates new connection if validation fails."""
        from etl.transfer.pool import FTPConnectionPool
        import ftplib

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp1 = MagicMock()
            mock_ftp2 = MagicMock()
            mock_ftp_class.side_effect = [mock_ftp1, mock_ftp2]

            # First connection becomes invalid
            mock_ftp1.voidcmd.side_effect = ftplib.error_temp("Connection lost")

            pool = FTPConnectionPool(mock_config, pool_size=2)

            conn1 = pool.borrow()
            pool.return_connection(conn1)

            # Borrow again - validation fails, new connection created
            conn2 = pool.borrow()

            assert conn2.ftp is mock_ftp2
            assert mock_ftp_class.call_count == 2

    def test_pool_close_all(self, mock_config):
        """Test close_all closes all connections."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            mock_ftp = MagicMock()
            mock_ftp_class.return_value = mock_ftp
            mock_ftp.voidcmd.return_value = None

            pool = FTPConnectionPool(mock_config, pool_size=2)

            conn = pool.borrow()
            pool.return_connection(conn)

            pool.close_all()

            mock_ftp.quit.assert_called()

    def test_pool_thread_safety(self, mock_config):
        """Test pool is thread-safe under concurrent access."""
        from etl.transfer.pool import FTPConnectionPool

        with patch("etl.transfer.pool.ftplib.FTP") as mock_ftp_class:
            # Create unique mock for each connection
            def create_mock():
                m = MagicMock()
                m.voidcmd.return_value = None
                return m

            mock_ftp_class.side_effect = create_mock

            pool = FTPConnectionPool(mock_config, pool_size=4, max_wait=5.0)
            results = []
            errors = []

            def worker(worker_id):
                try:
                    for _ in range(10):
                        conn = pool.borrow()
                        time.sleep(0.01)  # Simulate work
                        pool.return_connection(conn)
                        results.append(worker_id)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(errors) == 0
            assert len(results) == 40  # 4 workers * 10 iterations


class TestFTPPoolManager:
    """Test FTPPoolManager singleton class."""

    @pytest.fixture
    def mock_config(self):
        """Create mock server config."""
        config = MagicMock()
        config.host = "test.ftp.server"
        config.port = 21
        config.username = "testuser"
        config.password = "testpass"
        return config

    def test_get_pool_creates_new_pool(self, mock_config):
        """Test get_pool creates pool for new server."""
        from etl.transfer.pool import FTPPoolManager

        # Clear any existing pools
        FTPPoolManager._pools.clear()

        with patch("etl.transfer.pool.FTPConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            pool = FTPPoolManager.get_pool(mock_config)

            assert pool is mock_pool
            mock_pool_class.assert_called_once()

    def test_get_pool_returns_existing_pool(self, mock_config):
        """Test get_pool returns same pool for same server."""
        from etl.transfer.pool import FTPPoolManager

        # Clear any existing pools
        FTPPoolManager._pools.clear()

        with patch("etl.transfer.pool.FTPConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            pool1 = FTPPoolManager.get_pool(mock_config)
            pool2 = FTPPoolManager.get_pool(mock_config)

            assert pool1 is pool2
            # Only one pool created
            assert mock_pool_class.call_count == 1

    def test_get_pool_creates_separate_pools_for_different_servers(self):
        """Test get_pool creates separate pools for different servers."""
        from etl.transfer.pool import FTPPoolManager

        # Clear any existing pools
        FTPPoolManager._pools.clear()

        config1 = MagicMock()
        config1.host = "server1.ftp.com"
        config1.port = 21
        config1.username = "user1"

        config2 = MagicMock()
        config2.host = "server2.ftp.com"
        config2.port = 21
        config2.username = "user2"

        with patch("etl.transfer.pool.FTPConnectionPool") as mock_pool_class:
            mock_pool1 = MagicMock()
            mock_pool2 = MagicMock()
            mock_pool_class.side_effect = [mock_pool1, mock_pool2]

            pool1 = FTPPoolManager.get_pool(config1)
            pool2 = FTPPoolManager.get_pool(config2)

            assert pool1 is not pool2
            assert mock_pool_class.call_count == 2

    def test_close_all_closes_all_pools(self, mock_config):
        """Test close_all closes all managed pools."""
        from etl.transfer.pool import FTPPoolManager

        # Clear any existing pools
        FTPPoolManager._pools.clear()

        with patch("etl.transfer.pool.FTPConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            FTPPoolManager.get_pool(mock_config)
            FTPPoolManager.close_all()

            mock_pool.close_all.assert_called_once()

    def test_get_pool_thread_safety(self, mock_config):
        """Test get_pool is thread-safe."""
        from etl.transfer.pool import FTPPoolManager

        # Clear any existing pools
        FTPPoolManager._pools.clear()

        pools = []
        errors = []

        with patch("etl.transfer.pool.FTPConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            def get_pool_worker():
                try:
                    pool = FTPPoolManager.get_pool(mock_config)
                    pools.append(pool)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=get_pool_worker) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(errors) == 0
            # All threads should get the same pool
            assert all(p is pools[0] for p in pools)
            # Only one pool created
            assert mock_pool_class.call_count == 1
