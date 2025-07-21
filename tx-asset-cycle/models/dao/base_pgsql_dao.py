import logging
import random
import time
from types import TracebackType
from typing import Self, Type

import psycopg2
from psycopg2.extensions import connection as PgSqlConnection

from config.postgresql_config import PGSQL_DB_SETTINGS

logger = logging.getLogger(__name__)


class PgsqlConnectionError(Exception):
    """Custom exception for PostgreSQL connection-related errors.

    Extends the base Exception class to provide additional context for
    database connection failures, including the original exception and
    sanitized connection details for debugging purposes.

    Attributes:
        original_exception (Exception | None): The underlying exception that
            caused the connection failure.
        connection_details (str | None): Sanitized connection information
            for debugging (excludes sensitive data like passwords).

    Example:
        >>> try:
        ...     # Database connection attempt
        ...     pass
        ... except psycopg2.OperationalError as e:
        ...     raise PgsqlConnectionError(
        ...         "Connection failed",
        ...         original_exception=e,
        ...         connection_details="host=localhost..."
        ...     )
    """
    def __init__(self: Self, message: str,
                 original_exception: Exception | None = None,
                 connection_details: str | None = None) -> None:
        """Initialize the PgsqlConnectionError with additional context.

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


class PgsqlBaseDAO:
    """Base Data Access Object for PostgreSQL database operations.

    Provides a foundational class for PostgreSQL database interactions with
    robust connection management, automatic retry logic, and context manager
    support. Handles connection pooling, error recovery, and transaction
    management.

    Features:
        - Automatic connection retry with exponential backoff
        - Context manager support for proper resource cleanup
        - Transaction management with commit/rollback capabilities
        - Configurable retry parameters
        - Comprehensive error handling and logging

    Attributes:
        pgsql_db_settings (dict): Database connection configuration.
        max_retries (int): Maximum number of connection retry attempts.
        initial_backoff (int): Initial backoff time in seconds for retries.
        max_backoff (int): Maximum backoff time in seconds for retries.

    Example:
        >>> # Using as context manager
        >>> with PgsqlBaseDAO() as dao:
        ...     # Perform database operations
        ...     dao.commit()

        >>> # Direct usage
        >>> dao = PgsqlBaseDAO()
        >>> try:
        ...     connection = dao._get_connection()
        ...     # Database operations
        ...     dao.commit()
        ... finally:
        ...     dao.close_connection()
    """
    def __init__(self: Self) -> None:
        """Initialize the PostgreSQL DAO with default configuration.

        Sets up the database connection parameters and retry configuration
        from the imported settings. Initializes connection state and retry
        parameters with sensible defaults.
        """
        self.pgsql_db_settings = PGSQL_DB_SETTINGS
        self._conn: PgSqlConnection | None = None
        self.max_retries = 5
        self.initial_backoff = 1
        self.max_backoff = 16

    def _get_connection(self: Self) -> PgSqlConnection:
        """Establish and return a PostgreSQL database connection.

        Creates a new database connection or returns an existing one if
        still valid. Implements exponential backoff retry logic for
        transient connection failures. Automatically handles connection
        pooling and retry attempts for common network-related errors.

        The retry mechanism uses exponential backoff with jitter to avoid
        thundering herd problems. Only retries on specific recoverable
        errors like DNS resolution failures, connection refused, and
        timeouts.

        Returns:
            PgSqlConnection: Active PostgreSQL database connection with
                autocommit disabled for transaction management.

        Raises:
            PgsqlConnectionError: If connection fails after all retry
                attempts or encounters a non-retryable error.

        Note:
            This method sets autocommit=False to enable explicit transaction
            control through commit() and rollback() methods.
        """
        if self._conn is None or self._conn.closed:
            current_retry = 0
            backoff_time = self.initial_backoff
            last_exception = None
            while current_retry < self.max_retries:
                try:
                    self._conn = psycopg2.connect(**self.pgsql_db_settings)
                    self._conn.autocommit = False
                    return self._conn
                except psycopg2.OperationalError as e:
                    last_exception = e
                    error_message = str(e).lower()
                    retryable_errors = [
                        "could not translate host name",
                        "name or service not known",
                        "connection refused",
                        "timeout expired",
                    ]
                    if any(msg in error_message for msg in retryable_errors):
                        logger.warning(
                            "DB Connection attempt %s failed: %s. Retrying in %ss...",
                            current_retry + 1, e, backoff_time
                        )
                        jitter = random.uniform(0, 0.25 * backoff_time)
                        time.sleep(backoff_time + jitter)  # noqa: S311
                        backoff_time = min(backoff_time * 2, self.max_backoff)
                        current_retry += 1
                    else:
                        logger.error(
                            "ERROR (PgsqlBaseDAO): Could not connect to PostgreSQL "
                            "(non-retryable operational error): %s", e
                        )
                        raise PgsqlConnectionError(
                            "Failed to connect to database via psycopg2", e
                        ) from e
            # If all retries fail
            safe_details = f"host={self.pgsql_db_settings.get('host')}..."
            logger.error(f"ERROR: All {self.max_retries} "
                         "DB connection attempts failed.")
            raise PgsqlConnectionError(
                f"Failed to connect to DB after {self.max_retries} "
                f"attempts\n{last_exception}",
                original_exception=last_exception,
                connection_details=safe_details
            ) from last_exception
        return self._conn

    def close_connection(self: Self) -> None:
        """Close the active database connection and clean up resources.

        Safely closes the PostgreSQL connection if one exists and is open.
        Resets the internal connection reference to None and logs the
        closure for debugging purposes. This method is idempotent and safe
        to call multiple times.

        Note:
            This method should be called when database operations are
            complete to free up connection resources. It's automatically
            called when using the class as a context manager.
        """
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed by PgsqlBaseDAO.")

    def __enter__(self: Self) -> Self:
        """Enter the context manager and establish database connection.

        Called when entering a 'with' statement. Establishes the database
        connection and returns the DAO instance for use within the context.

        Returns:
            Self: The DAO instance with an active database connection.

        Example:
            >>> with PgsqlBaseDAO() as dao:
            ...     # dao now has an active connection
            ...     pass  # Connection automatically closed on exit
        """
        self._get_connection()
        return self

    def __exit__(
        self: Self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        """Exit the context manager and clean up database connection.

        Called when exiting a 'with' statement. Automatically closes the
        database connection regardless of whether an exception occurred
        within the context. Ensures proper resource cleanup.

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
        self.close_connection()

    def commit(self: Self) -> None:
        """Commit the current database transaction.

        Commits all pending changes in the current transaction to the
        database. Only performs the commit if an active connection exists
        and is open. This method should be called after successful
        completion of database operations to persist changes.

        Note:
            This method is safe to call even if no transaction is active.
            If the connection is closed or None, the method returns without
            action.

        Example:
            >>> dao = PgsqlBaseDAO()
            >>> try:
            ...     # Perform database operations
            ...     dao.commit()  # Persist changes
            ... except Exception:
            ...     dao.rollback()  # Undo changes on error
        """
        if self._conn and not self._conn.closed:
            self._conn.commit()

    def rollback(self: Self) -> None:
        """Roll back the current database transaction.

        Undoes all pending changes in the current transaction, reverting
        the database to its state at the beginning of the transaction.
        Only performs the rollback if an active connection exists and is
        open. Typically used in error handling to maintain data consistency.

        Note:
            This method is safe to call even if no transaction is active.
            If the connection is closed or None, the method returns without
            action.

        Example:
            >>> dao = PgsqlBaseDAO()
            >>> try:
            ...     # Perform database operations
            ...     dao.commit()
            ... except Exception:
            ...     dao.rollback()  # Undo changes on error
            ...     raise  # Re-raise the exception
        """
        if self._conn and not self._conn.closed:
            self._conn.rollback()
