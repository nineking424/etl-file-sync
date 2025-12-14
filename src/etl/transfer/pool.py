"""FTP connection pool implementation."""

import ftplib
import logging
import queue
import threading
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from etl.config import ServerConfig

logger = logging.getLogger(__name__)


class PooledConnection:
    """Wrapper for a pooled FTP connection.

    Tracks connection metadata and provides context manager support
    for automatic return to pool.
    """

    def __init__(self, ftp: ftplib.FTP, pool: "FTPConnectionPool"):
        """Initialize pooled connection.

        Args:
            ftp: The underlying FTP connection
            pool: The pool this connection belongs to
        """
        self.ftp = ftp
        self._pool = pool
        self.created_at = time.time()
        self.last_used = time.time()

    def __enter__(self) -> ftplib.FTP:
        """Context manager entry - return FTP connection."""
        return self.ftp

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - return connection to pool."""
        self._pool.return_connection(self)


class FTPConnectionPool:
    """Thread-safe FTP connection pool.

    Manages a pool of FTP connections to a single server,
    providing connection reuse to reduce overhead and prevent
    connection exhaustion.
    """

    def __init__(
        self,
        config: "ServerConfig",
        pool_size: int = 4,
        timeout: int = 30,
        passive_mode: bool = True,
        max_wait: float = 30.0,
        idle_timeout: float = 300.0,
    ):
        """Initialize connection pool.

        Args:
            config: Server configuration
            pool_size: Maximum number of connections in pool
            timeout: Connection timeout in seconds
            passive_mode: Use FTP passive mode
            max_wait: Maximum time to wait for available connection
            idle_timeout: Close connections idle longer than this
        """
        self._config = config
        self._pool_size = pool_size
        self._timeout = timeout
        self._passive_mode = passive_mode
        self._max_wait = max_wait
        self._idle_timeout = idle_timeout

        self._pool: queue.Queue[PooledConnection] = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created_count = 0
        self._closed = False

        logger.info(
            f"FTPConnectionPool initialized for {config.host}:{config.port} "
            f"(pool_size={pool_size})"
        )

    def borrow(self) -> PooledConnection:
        """Borrow a connection from the pool.

        Returns an existing connection from the pool if available,
        or creates a new one if pool is not at capacity.
        Blocks if pool is exhausted until a connection becomes available
        or max_wait timeout is reached.

        Returns:
            PooledConnection wrapper around FTP connection

        Raises:
            TimeoutError: If no connection available within max_wait
            ConnectionError: If connection creation fails
        """
        if self._closed:
            raise ConnectionError("Pool is closed")

        # Try to get existing connection from pool
        try:
            conn = self._pool.get_nowait()
            # Validate the connection
            if self._validate_connection(conn.ftp):
                conn.last_used = time.time()
                logger.debug(f"Reusing pooled connection to {self._config.host}")
                return conn
            else:
                # Connection is stale, close and create new
                logger.debug("Pooled connection invalid, creating new one")
                self._close_connection(conn.ftp)
                with self._lock:
                    self._created_count -= 1
        except queue.Empty:
            pass

        # Try to create new connection if under pool size
        with self._lock:
            if self._created_count < self._pool_size:
                self._created_count += 1
                try:
                    ftp = self._create_connection()
                    conn = PooledConnection(ftp, self)
                    logger.debug(
                        f"Created new pooled connection to {self._config.host} "
                        f"({self._created_count}/{self._pool_size})"
                    )
                    return conn
                except Exception:
                    self._created_count -= 1
                    raise

        # Pool is at capacity, wait for available connection
        logger.debug(
            f"Pool exhausted, waiting for available connection "
            f"(max_wait={self._max_wait}s)"
        )
        try:
            conn = self._pool.get(timeout=self._max_wait)
            # Validate the connection
            if self._validate_connection(conn.ftp):
                conn.last_used = time.time()
                return conn
            else:
                # Connection is stale, close and create new
                self._close_connection(conn.ftp)
                with self._lock:
                    self._created_count -= 1
                # Recursively try to get another connection
                return self.borrow()
        except queue.Empty:
            raise TimeoutError(
                f"No connection available within {self._max_wait}s timeout"
            )

    def return_connection(self, conn: PooledConnection) -> None:
        """Return a connection to the pool.

        Args:
            conn: The pooled connection to return
        """
        if self._closed:
            self._close_connection(conn.ftp)
            return

        conn.last_used = time.time()

        try:
            self._pool.put_nowait(conn)
            logger.debug(f"Connection returned to pool for {self._config.host}")
        except queue.Full:
            # Pool is full, close the connection
            logger.debug("Pool full, closing excess connection")
            self._close_connection(conn.ftp)
            with self._lock:
                self._created_count -= 1

    def _create_connection(self) -> ftplib.FTP:
        """Create a new FTP connection.

        Returns:
            Connected FTP instance

        Raises:
            ConnectionError: If connection fails
        """
        try:
            ftp = ftplib.FTP()
            ftp.connect(self._config.host, self._config.port, timeout=self._timeout)
            ftp.login(self._config.username, self._config.password)
            ftp.set_pasv(self._passive_mode)

            mode = "passive" if self._passive_mode else "active"
            logger.info(
                f"FTP connection established to {self._config.host}:{self._config.port} "
                f"({mode} mode)"
            )
            return ftp

        except ftplib.all_errors as e:
            raise ConnectionError(
                f"Failed to connect to FTP server "
                f"{self._config.host}:{self._config.port}: {e}"
            )

    def _validate_connection(self, ftp: ftplib.FTP) -> bool:
        """Validate a connection is still alive.

        Uses NOOP command to check connection health.

        Args:
            ftp: FTP connection to validate

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            ftp.voidcmd("NOOP")
            return True
        except ftplib.all_errors:
            return False

    def _close_connection(self, ftp: ftplib.FTP) -> None:
        """Close an FTP connection safely.

        Args:
            ftp: FTP connection to close
        """
        try:
            ftp.quit()
        except ftplib.all_errors:
            try:
                ftp.close()
            except Exception:
                pass

    def close_all(self) -> None:
        """Close all connections in the pool."""
        self._closed = True

        while True:
            try:
                conn = self._pool.get_nowait()
                self._close_connection(conn.ftp)
            except queue.Empty:
                break

        with self._lock:
            self._created_count = 0

        logger.info(f"All connections closed for {self._config.host}:{self._config.port}")


class FTPPoolManager:
    """Global manager for FTP connection pools.

    Maintains a singleton pool per server (identified by host:port:username).
    Provides centralized pool lifecycle management.
    """

    _pools: dict[str, FTPConnectionPool] = {}
    _lock = threading.Lock()

    @classmethod
    def get_pool(
        cls,
        config: "ServerConfig",
        pool_size: int = 4,
        timeout: int = 30,
        passive_mode: bool = True,
        max_wait: float = 30.0,
        idle_timeout: float = 300.0,
    ) -> FTPConnectionPool:
        """Get or create a connection pool for a server.

        Args:
            config: Server configuration
            pool_size: Maximum connections per pool
            timeout: Connection timeout
            passive_mode: Use FTP passive mode
            max_wait: Max wait for available connection
            idle_timeout: Idle connection timeout

        Returns:
            Connection pool for the specified server
        """
        key = f"{config.host}:{config.port}:{config.username}"

        with cls._lock:
            if key not in cls._pools:
                cls._pools[key] = FTPConnectionPool(
                    config=config,
                    pool_size=pool_size,
                    timeout=timeout,
                    passive_mode=passive_mode,
                    max_wait=max_wait,
                    idle_timeout=idle_timeout,
                )
                logger.info(f"Created new FTP pool for {key}")
            return cls._pools[key]

    @classmethod
    def close_all(cls) -> None:
        """Close all managed connection pools."""
        with cls._lock:
            for key, pool in cls._pools.items():
                logger.info(f"Closing pool for {key}")
                pool.close_all()
            cls._pools.clear()

    @classmethod
    def clear(cls) -> None:
        """Clear all pools (for testing)."""
        cls.close_all()
