import logging
import time
from types import TracebackType
from typing import Self, Type

import redis
from redis import Redis

from config.redis_config import REDIS_DB_SETTINGS

logger = logging.getLogger(__name__)


class RedisConnectionError(Exception):
    """Custom exception for Redis connection-related errors.

    Extends the base Exception class to provide additional context for
    Redis connection failures, including the original exception and
    sanitized connection details for debugging purposes.

    Attributes:
        original_exception (Exception | None): The underlying exception that
            caused the connection failure.
        connection_details (str | None): Sanitized connection information
            for debugging (excludes sensitive data like passwords).

    Example:
        >>> try:
        ...     # Redis connection attempt
        ...     pass
        ... except redis.ConnectionError as e:
        ...     raise RedisConnectionError(
        ...         "Connection failed",
        ...         original_exception=e,
        ...         connection_details="host=localhost:6379..."
        ...     )
    """
    def __init__(self: Self, message: str,
                 original_exception: Exception | None = None,
                 connection_details: str | None = None) -> None:
        """Initialize the RedisConnectionError with additional context.

        Args:
            message (str): Primary error message describing the failure.
            original_exception (Exception | None): The underlying exception
                that caused this error. Defaults to None.
            connection_details (str | None): Sanitized connection details
                for debugging. Should not contain sensitive information.
                Defaults to None.
        """
        super().__init__(message)
        self.original_exception = original_exception
        self.connection_details = connection_details

    def __str__(self: Self) -> str:
        """Return a string representation including connection details.

        Extends the base exception string representation to include
        connection details when available, providing more context for
        debugging connection issues.

        Returns:
            str: Formatted error message with optional connection details.
        """
        base_message = super().__str__()
        if self.connection_details:
            base_message += f" (Connection Details: {self.connection_details})"
        return base_message


class RedisBaseDAO:
    """Base Data Access Object for Redis database operations.

    Provides a foundational class for Redis database interactions with
    robust connection management, automatic retry logic, and context manager
    support. Handles connection pooling, error recovery, and health checking.

    Features:
        - Automatic connection retry with exponential backoff
        - Context manager support for proper resource cleanup
        - SSL/TLS connection support
        - Health monitoring with periodic ping checks
        - Configurable timeout and retry parameters
        - Comprehensive error handling and logging

    Attributes:
        host (str): Redis server hostname or IP address.
        port (int): Redis server port number.
        username (str): Redis authentication username.
        password (str): Redis authentication password.
        db (int): Redis database number (default: 0).
        decode_responses (bool): Whether to decode responses to strings.
        max_retries (int): Maximum number of connection retry attempts.
        retry_backoff (int): Base backoff multiplier for retry delays.
        ssl_state (bool): Whether to use SSL/TLS connection.

    Example:
        >>> # Using as context manager
        >>> with RedisBaseDAO() as dao:
        ...     conn = dao._get_connection()
        ...     # Perform Redis operations

        >>> # Direct usage
        >>> dao = RedisBaseDAO()
        >>> try:
        ...     connection = dao._get_connection()
        ...     # Redis operations
        ... finally:
        ...     dao.close()
    """
    def __init__(self: Self) -> None:
        """Initialize the Redis DAO with configuration from settings.

        Sets up the Redis connection parameters from the imported settings
        and initializes retry configuration with sensible defaults.
        Configures SSL, timeouts, and health check parameters for robust
        connection management.
        """
        self.host = REDIS_DB_SETTINGS.get("host")
        self.port = REDIS_DB_SETTINGS.get("port")
        self.username = REDIS_DB_SETTINGS.get("username")
        self.password = REDIS_DB_SETTINGS.get("password")
        self.db = 0
        self.decode_responses = True
        self.max_retries = 3
        self.retry_backoff = 2
        self.ssl_state = True
        self._redis_conn : Redis | None = None

    def _connect(self: Self) -> None:
        """Establish a new Redis connection with retry logic.

        Creates a new Redis connection using the configured parameters and
        implements exponential backoff retry logic for connection failures.
        Performs a ping test to verify the connection is working before
        returning. Uses progressively longer delays between retry attempts.

        The connection includes SSL support, configurable timeouts, and
        health check intervals for robust operation in production
        environments.

        Raises:
            RedisConnectionError: If connection fails after all retry
                attempts have been exhausted.

        Note:
            This method sets socket timeouts and health check intervals
            for reliable long-running connections. The ping test ensures
            the connection is immediately usable.
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                logger.info(
                    f"Connecting to Redis (attempt {attempt + 1}).."
                )
                self._redis_conn = Redis(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    db=self.db,
                    decode_responses=self.decode_responses,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    health_check_interval=30,
                    ssl=self.ssl_state
                )
                self._redis_conn.ping()
                logger.info("Connected to Redis.")
                return
            except redis.ConnectionError as e:
                logger.warning(
                    f"Failed to connect to Redis: {e}. "
                    f"Retrying in {self.retry_backoff ** (attempt + 1)}s.."
                )
                time.sleep(self.retry_backoff ** (attempt + 1))
                attempt += 1
        raise RedisConnectionError("Failed to connect to Redis after multiple retries.")

    def _get_connection(self: Self) -> Redis:
        """Get an active Redis connection, reconnecting if necessary.

        Returns an existing Redis connection or creates a new one if none
        exists. Performs a health check using ping() to verify the
        connection is still active. Automatically reconnects if the
        connection has been lost or is unresponsive.

        This method implements connection pooling by reusing existing
        connections when possible and only creating new connections when
        needed or when the existing connection has failed.

        Returns:
            Redis: Active Redis connection instance ready for operations.

        Raises:
            RedisConnectionError: If connection establishment or
                reconnection fails after all retry attempts.

        Example:
            >>> dao = RedisBaseDAO()
            >>> redis_conn = dao._get_connection()
            >>> redis_conn.set("key", "value")
        """
        try:
            if self._redis_conn is None:
                self._connect()
            else:
                self._redis_conn.ping()
            return self._redis_conn
        except redis.ConnectionError as e:
            logger.error(f"Redis connection lost: {e}. Reconnecting...")
            self._connect()
            return self._redis_conn

    def __enter__(self: Self) -> Self:
        """Enter the context manager and establish Redis connection.

        Called when entering a 'with' statement. Establishes the Redis
        connection and returns the DAO instance for use within the context.

        Returns:
            Self: The DAO instance with an active Redis connection.

        Example:
            >>> with RedisBaseDAO() as dao:
            ...     # dao now has an active connection
            ...     conn = dao._get_connection()
            ...     # Connection automatically closed on exit
        """
        self._get_connection()
        return self

    def close(self: Self) -> None:
        """Close the active Redis connection and clean up resources.

        Safely closes the Redis connection if one exists. Resets the
        internal connection reference and logs the closure for debugging
        purposes. This method is idempotent and safe to call multiple times.

        Note:
            This method should be called when Redis operations are complete
            to free up connection resources. It's automatically called when
            using the class as a context manager.
        """
        if self._redis_conn:
            self._redis_conn.close()
            logger.info("Redis connection closed.")

    def __exit__(
        self: Self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        """Exit the context manager and clean up Redis connection.

        Called when exiting a 'with' statement. Automatically closes the
        Redis connection regardless of whether an exception occurred within
        the context. Ensures proper resource cleanup.

        Args:
            exc_type (Type[BaseException] | None): The exception type if an
                exception was raised in the context, None otherwise.
            exc_val (BaseException | None): The exception instance if an
                exception was raised in the context, None otherwise.
            exc_tb (TracebackType | None): The traceback object if an
                exception was raised in the context, None otherwise.

        Note:
            This method does not suppress exceptions - any exceptions raised
            within the context will continue to propagate after cleanup.
        """
        self.close()
