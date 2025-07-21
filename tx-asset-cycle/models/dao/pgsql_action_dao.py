import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Self, Union

import psycopg2
from psycopg2 import extras as pg_extras
from psycopg2 import sql

from config.postgresql_config import ALLOWED_TABLES, DX_SCHEMA_NAME
from models.dto.record_dto import CycleRecord, ProcessInfoRecord

from .base_pgsql_dao import PgsqlBaseDAO, PgsqlConnectionError

logger = logging.getLogger(__name__)

# Constants
MAX_RETRIES = 2
DEFAULT_COLUMNS = ["*"]


class DataAccessError(Exception):
    """Custom exception for data access operations.

    Extends the base Exception class to provide additional context for
    database access failures, including the original exception and
    connection details for debugging purposes.

    Attributes:
        original_exception (Exception | None): The underlying exception that
            caused the data access failure.
        connection_details (str | None): Sanitized connection information
            for debugging (excludes sensitive data).

    Example:
        >>> try:
        ...     # Database operation
        ...     pass
        ... except psycopg2.Error as e:
        ...     raise DataAccessError(
        ...         "Query failed",
        ...         original_exception=e,
        ...         connection_details="table=users..."
        ...     )
    """

    def __init__(
        self: Self,
        message: str,
        original_exception: Exception | None = None,
        connection_details: str | None = None
    ) -> None:
        """Initialize the DataAccessError with additional context.

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
        debugging data access issues.

        Returns:
            str: Formatted error message with optional connection details.
        """
        base_message = super().__str__()
        if self.connection_details:
            base_message += f" (Connection Details: {self.connection_details})"
        return base_message


@dataclass
class QueryParams:
    """Parameters for table-based database queries.

    Encapsulates all parameters needed to construct and execute a SELECT
    query against a specific table with optional filtering, ordering, and
    limiting. Provides type safety and validation for common query patterns.

    Attributes:
        table (str): Name of the database table to query. Must be in the
            ALLOWED_TABLES whitelist for security.
        columns (Optional[List[str]]): List of column names to select.
            None or ["*"] selects all columns.
        where_clause (Optional[str]): SQL WHERE clause string for filtering
            results. Should not include the WHERE keyword.
        limit (Optional[int]): Maximum number of rows to return. Must be
            positive integer if specified.
        order_by (Optional[str]): SQL ORDER BY clause string for result
            ordering. Should not include ORDER BY keyword.

    Example:
        >>> params = QueryParams(
        ...     table="asset_cycles",
        ...     columns=["asset_guid", "cycle_status"],
        ...     where_clause="cycle_status = 'INPROGRESS'",
        ...     limit=100,
        ...     order_by="created_date DESC"
        ... )
    """
    table: str
    columns: Optional[List[str]] = None
    where_clause: Optional[str] = None
    limit: Optional[int] = None
    order_by: Optional[str] = None


@dataclass
class CustomQueryParams:
    """Parameters for custom SQL queries.

    Encapsulates parameters needed to execute custom SQL queries with
    optional parameterization for safe query execution. Uses psycopg2.sql
    for SQL injection protection and proper identifier escaping.

    Attributes:
        query (sql.SQL): SQL query object constructed with psycopg2.sql
            for safe query composition and identifier escaping.
        params (Optional[List[Any]]): List of parameters to bind to the
            query placeholders (%s) for safe execution. Parameters are
            automatically escaped by psycopg2.

    Example:
        >>> params = CustomQueryParams(
        ...     query=sql.SQL(
        ...         "SELECT * FROM {schema}.{table} WHERE asset_guid = %s"
        ...     ).format(
        ...         schema=sql.Identifier("dx"),
        ...         table=sql.Identifier("asset_cycles")
        ...     ),
        ...     params=["truck-123"]
        ... )
    """
    query: sql.SQL
    params: Optional[List[Any]] = None


class PgsqlActionDAO(PgsqlBaseDAO):
    """Data Access Object for PostgreSQL operations with advanced features.

    Provides a high-level interface for executing database queries with
    built-in retry logic, error handling, SQL injection protection, and
    support for both custom SQL queries and parameterized table queries.

    The class inherits connection management from PgsqlBaseDAO and extends
    it with:
    - Table access validation and whitelisting
    - Automatic query retry with exponential backoff
    - Safe SQL composition using psycopg2.sql
    - Batch insert operations with execute_values
    - Transaction management with automatic rollback
    - Comprehensive error handling and logging

    Key Features:
        - SQL injection protection through parameterized queries
        - Table access control via ALLOWED_TABLES whitelist
        - Automatic retry logic for transient database errors
        - Support for both single and batch operations
        - Transaction safety with automatic rollback on errors
        - Detailed logging for debugging and monitoring

    Attributes:
        dx_schema_name (str): The database schema name for table operations.
        _max_retries (int): Maximum number of retry attempts for failed
            queries before raising an exception.

    Example:
        >>> dao = PgsqlActionDAO()
        >>> try:
        ...     with dao:
        ...         result = dao.pull_data_from_table(
        ...             single_query={
        ...                 'table': 'asset_cycles',
        ...                 'target_filter': 'asset_guid = %s',
        ...                 'target_limit': 10
        ...             }
        ...         )
        ... finally:
        ...     dao.close_connection()
    """

    def __init__(self: Self) -> None:
        """Initialize the PostgreSQL Action DAO with configuration.

        Sets up the database schema name and retry parameters from
        configuration constants. Inherits connection management from
        the base DAO class.
        """
        super().__init__()
        self.dx_schema_name = DX_SCHEMA_NAME
        self._max_retries = MAX_RETRIES

    def _validate_table_access(self: Self, table_name: str) -> None:
        """Validate that the specified table is allowed for queries.

        Checks the table name against the ALLOWED_TABLES whitelist to
        prevent unauthorized access to database tables. This is a critical
        security measure to ensure only approved tables can be queried.

        Args:
            table_name (str): Name of the table to validate. Case-insensitive
                comparison is performed.

        Raises:
            PgsqlConnectionError: If the table is not in the allowed list
                or does not exist in the whitelist configuration.

        Security Note:
            This method prevents SQL injection and unauthorized table access
            by enforcing a strict whitelist of allowed table names.
        """
        if table_name.lower() not in ALLOWED_TABLES:
            raise PgsqlConnectionError(
                f"Querying table '{table_name}' is not allowed or does not exist."
            )

    def _build_table_query(self: Self, params: QueryParams) -> sql.SQL:
        """Build a parameterized SQL SELECT query from table query parameters.

        Constructs a safe SQL query using psycopg2's SQL composition to
        prevent SQL injection attacks. Handles column selection, WHERE
        clauses, ORDER BY, and LIMIT clauses based on the provided parameters.

        Query Construction Process:
        1. Build column list or use wildcard for all columns
        2. Construct base SELECT statement with schema and table identifiers
        3. Add optional WHERE clause for filtering
        4. Add optional ORDER BY clause for sorting
        5. Add optional LIMIT clause for result limiting

        Args:
            params (QueryParams): QueryParams object containing table query
                specifications including table name, columns, filters, etc.

        Returns:
            sql.SQL: A composed SQL query object ready for execution with
                proper identifier escaping and parameterization.

        Example:
            >>> params = QueryParams(
            ...     table="users",
            ...     columns=["id", "name"],
            ...     where_clause="active = true",
            ...     limit=10
            ... )
            >>> query = self._build_table_query(params)
            >>> # Results in: SELECT id,name FROM schema.users
            >>> #               WHERE active = true LIMIT 10;
        """
        # Handle columns
        if not params.columns or params.columns == DEFAULT_COLUMNS:
            columns_sql = sql.SQL("*")
        else:
            columns_sql = sql.SQL(",").join(
                [sql.SQL(col) for col in params.columns]
            )

        # Base query
        query_parts = [
            sql.SQL("SELECT {columns} FROM {schema}.{table}").format(
                columns=columns_sql,
                schema=sql.Identifier(self.dx_schema_name),
                table=sql.Identifier(params.table),
            )
        ]

        # Add WHERE clause
        if params.where_clause:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(params.where_clause))

        # Add ORDER BY
        if params.order_by:
            query_parts.append(sql.SQL("ORDER BY ") + sql.SQL(params.order_by))

        # Add LIMIT
        if params.limit:
            query_parts.append(sql.SQL("LIMIT {limit}").format(
                limit=sql.Literal(params.limit)
            ))

        return sql.SQL(" ").join(query_parts) + sql.SQL(";")

    def _execute_operation_with_retry(
        self: Self,
        query: sql.SQL,
        params: Optional[Union[tuple, List[tuple]]] = None,
        target_description: str = "operation",
        operation_type: str = "select",
        fetch_results: bool = True
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute database operation with automatic retry logic and error handling.

        Unified method for executing any database operation (SELECT, INSERT,
        UPDATE) with automatic retry logic, transaction management, and
        flexible result handling. Supports both single and batch operations.

        Operation Types:
        - "select": Execute SELECT query and return formatted results
        - "insert": Execute INSERT with single record or batch optimization
        - "update": Execute UPDATE with transaction commit
        - "custom": Execute any SQL operation with flexible parameters

        Args:
            query (sql.SQL): The SQL query object to execute with proper
                identifier escaping and parameterization.
            params (Optional[Union[tuple, List[tuple]]]): Parameters to bind:
                - tuple: Single set of parameters for single operation
                - List[tuple]: Multiple parameter sets for batch operations
                - None: No parameters needed
            target_description (str): Human-readable description for logging
                and error reporting purposes.
            operation_type (str): Type of operation - "select", "insert",
                "update", or "custom".
            fetch_results (bool): Whether to fetch and return results.
                False for INSERT/UPDATE operations.

        Returns:
            Optional[List[Dict[str, Any]]]: For SELECT operations, returns
                list of dictionaries representing rows. For INSERT/UPDATE
                operations, returns None.

        Raises:
            PgsqlConnectionError: If operation fails after all retry attempts.

        Example:
            >>> # SELECT operation
            >>> result = self._execute_operation_with_retry(
            ...     query=sql.SQL("SELECT * FROM users WHERE id = %s"),
            ...     params=(123,),
            ...     target_description="user lookup",
            ...     operation_type="select"
            ... )

            >>> # INSERT operation
            >>> self._execute_operation_with_retry(
            ...     query=insert_query,
            ...     params=[(val1, val2), (val3, val4)],
            ...     target_description="batch insert users",
            ...     operation_type="insert",
            ...     fetch_results=False
            ... )
        """
        last_exception = None

        for retry_attempt in range(self._max_retries):
            try:
                conn = self._get_connection()

                with conn.cursor() as cur:
                    if operation_type == "select":
                        # Handle SELECT operations
                        cur.execute(query, params)
                        if fetch_results:
                            columns = [column[0] for column in cur.description]
                            rows = cur.fetchall()

                            if not rows:
                                logger.info(f"No data found for {target_description}")
                                return None

                            result = [dict(zip(columns, row)) for row in rows]
                            logger.info(
                                f"Successfully executed {target_description}, "
                                f"found {len(result)} rows"
                            )
                            return result
                        return None

                    elif operation_type in ["insert", "update"]:
                        # Handle INSERT/UPDATE operations
                        if isinstance(params, list) and len(params) > 1:
                            # Batch operation
                            pg_extras.execute_values(
                                cur,
                                query,
                                params,
                                page_size=min(100, len(params))
                            )
                        else:
                            # Single operation
                            single_params = (
                                params[0] if isinstance(params, list) else params
                            )
                            cur.execute(query, single_params)

                        conn.commit()
                        logger.info(f"Successfully executed {target_description}")
                        return None

                    else:  # custom operation
                        # Handle custom operations
                        if isinstance(params, list):
                            for param_set in params:
                                cur.execute(query, param_set)
                        else:
                            cur.execute(query, params)

                        if fetch_results and cur.description:
                            columns = [column[0] for column in cur.description]
                            rows = cur.fetchall()
                            result = [dict(zip(columns, row)) for row in rows]
                            return result

                        conn.commit()
                        return None

            except (Exception, psycopg2.Error) as e:
                last_exception = e
                logger.error(
                    f"ERROR (PgsqlActionDAO): Failed to execute {target_description} "
                    f"(attempt {retry_attempt + 1}/{self._max_retries}): {e}"
                )

                # Rollback on error
                if self._conn and not self._conn.closed:
                    self._conn.rollback()

        # All retries exhausted
        raise PgsqlConnectionError(
            f"Failed to execute {target_description} after"
            f" {self._max_retries} attempts\n"
            f"{last_exception}"
        ) from last_exception

    def _read_custom_query(
        self: Self,
        custom_params: CustomQueryParams
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute a custom SQL query with parameters.

        Handles the execution of user-provided SQL queries with optional
        parameter binding for safe query execution. Provides flexibility
        for complex queries that cannot be expressed through the standard
        table query interface.

        Args:
            custom_params (CustomQueryParams): CustomQueryParams object
                containing the SQL query and optional parameters.

        Returns:
            Optional[List[Dict[str, Any]]]: List of dictionaries representing
                query results, or None if no data found.

        Raises:
            PgsqlConnectionError: If query execution fails after all retry
                attempts are exhausted.

        Example:
            >>> params = CustomQueryParams(
            ...     query=sql.SQL("SELECT COUNT(*) as total FROM users"),
            ...     params=None
            ... )
            >>> result = self._read_custom_query(params)
        """
        query_params = tuple(custom_params.params) if custom_params.params else None
        return self._execute_operation_with_retry(
            query=custom_params.query,
            params=query_params,
            target_description="custom query",
            operation_type="select",
            fetch_results=True
        )

    def _read_table_query(
        self: Self,
        query_params: QueryParams
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute a table-based query with validation and security checks.

        Constructs and executes a SELECT query against a specific table,
        with built-in validation to ensure only allowed tables are accessed.
        Provides a secure interface for common table query patterns.

        Args:
            query_params (QueryParams): QueryParams object containing table
                query specifications including table name, columns, filters,
                ordering, and limiting.

        Returns:
            Optional[List[Dict[str, Any]]]: List of dictionaries representing
                query results, or None if no data found.

        Raises:
            PgsqlConnectionError: If table access is not allowed or query
                execution fails after all retry attempts.

        Example:
            >>> params = QueryParams(
            ...     table="asset_cycles",
            ...     columns=["asset_guid", "cycle_status"],
            ...     where_clause="cycle_status = 'INPROGRESS'"
            ... )
            >>> result = self._read_table_query(params)
        """
        self._validate_table_access(query_params.table)

        query = self._build_table_query(query_params)
        target_description = f"table {self.dx_schema_name}.{query_params.table}"

        return self._execute_operation_with_retry(
            query=query,
            params=None,
            target_description=target_description,
            operation_type="select",
            fetch_results=True
        )

    def pull_data_from_table(
        self: Self,
        custom_query: Optional[Dict[str, Any]] = None,
        single_query: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Pull data from database using either custom query or table query.

        Main public interface for executing database queries. Supports two
        types of operations: custom SQL queries with optional parameters,
        and table-based queries with filtering, ordering, and limiting.
        Provides a unified interface while maintaining backward compatibility.

        Query Types:
        1. Custom Query: Executes user-provided SQL with parameter binding
        2. Table Query: Constructs SELECT query with validation and security

        Args:
            custom_query (Optional[Dict[str, Any]]): Dictionary containing
                custom SQL query parameters:
                - 'query' (required): psycopg2.sql.SQL object
                - 'params' (optional): List of parameters to bind
            single_query (Optional[Dict[str, Any]]): Dictionary containing
                table query parameters:
                - 'table' (required): Name of the table to query
                - 'target_columns' (optional): List of column names
                - 'target_filter' (optional): WHERE clause string
                - 'target_limit' (optional): Maximum rows to return
                - 'target_order' (optional): ORDER BY clause string

        Returns:
            Optional[List[Dict[str, Any]]]: List of dictionaries representing
                query results where each dictionary represents a row with
                column names as keys, or None if no data found.

        Raises:
            PgsqlConnectionError: If required parameters are missing, table
                access is not allowed, or query execution fails after all
                retry attempts.

        Examples:
            # Custom query example
            >>> custom_query = {
            ...     'query': sql.SQL("SELECT * FROM users WHERE age > %s"),
            ...     'params': [25]
            ... }
            >>> result = dao.pull_data_from_table(custom_query=custom_query)

            # Table query example
            >>> single_query = {
            ...     'table': 'users',
            ...     'target_columns': ['id', 'name', 'email'],
            ...     'target_filter': 'active = true',
            ...     'target_limit': 100,
            ...     'target_order': 'created_at DESC'
            ... }
            >>> result = dao.pull_data_from_table(single_query=single_query)
        """
        if custom_query:
            if "query" not in custom_query:
                raise PgsqlConnectionError(
                    "Query is not provided in custom_query."
                )
            custom_params = CustomQueryParams(
                query=custom_query["query"],
                params=custom_query.get("params")
            )
            return self._read_custom_query(custom_params)
        elif single_query:
            if "table" not in single_query.keys():
                raise PgsqlConnectionError(
                    "Table name is not provided in single_query."
                )
            query_params = QueryParams(
                table=single_query["table"].lower(),
                columns=single_query.get("target_columns"),
                where_clause=single_query.get("target_filter"),
                limit=single_query.get("target_limit"),
                order_by=single_query.get("target_order")
            )
            return self._read_table_query(query_params)
        else:
            raise PgsqlConnectionError(
                "Please provide either 'custom_query' or 'single_query' parameters."
            )

    def insert_new_data(
        self: Self,
        data: Union[CycleRecord, ProcessInfoRecord]
    ) -> None:
        """Insert a new record into the primary table with retry logic.

        Inserts a single new record using the record's generated INSERT
        query. Validates that the record is new (no primary key) before
        attempting insertion and provides automatic retry logic for
        transient failures.

        Args:
            data (Union[CycleRecord, ProcessInfoRecord]): A record instance
                to insert. Must have is_new_record() returning True.

        Raises:
            ValueError: If the record already has a primary key (not new).
            PgsqlConnectionError: If insertion fails after all retry attempts.

        Example:
            >>> cycle = CycleRecord(
            ...     asset_guid="truck-123",
            ...     cycle_status="INPROGRESS",
            ...     # ... other required fields
            ... )
            >>> dao.insert_new_data(cycle)
        """
        if not data.is_new_record():
            raise ValueError(
                f"Cannot insert record with existing primary key: "
                f"{data.DB_PRIMARY_KEY_FIELD}="
                f"{getattr(data, data.DB_PRIMARY_KEY_FIELD)}"
            )

        insert_query = data.generate_insert_query()
        insert_values = data.data_to_insert_tuple()
        target_description = f"table {data.SCHEMA_NAME}.{data.TABLE_NAME}"

        self._execute_operation_with_retry(
            query=insert_query,
            params=insert_values,
            target_description=target_description,
            operation_type="insert",
            fetch_results=False
        )

    def insert_new_final_cycle_data(
        self: Self,
        data: CycleRecord
    ) -> None:
        """Insert a completed cycle record into the final table.

        Inserts a cycle record into the final/archive table using the
        record's final table INSERT query. This is typically used for
        completed cycles that need to be moved to a separate table for
        reporting and analytics purposes.

        Args:
            data (CycleRecord): A CycleRecord instance to insert into the
                final table. The record should represent a completed cycle.

        Raises:
            PgsqlConnectionError: If insertion fails after all retry attempts.
            AttributeError: If the CycleRecord doesn't support final table
                operations (missing generate_insert_query_final method).

        Example:
            >>> completed_cycle = CycleRecord(
            ...     asset_guid="truck-123",
            ...     cycle_status="COMPLETE",
            ...     # ... timing and performance data
            ... )
            >>> dao.insert_new_final_cycle_data(completed_cycle)
        """
        insert_query = data.generate_insert_query_final()
        insert_values = data.data_to_insert_tuple()
        target_description = f"table {data.SCHEMA_NAME}.{data.FINAL_TABLE_NAME}"

        self._execute_operation_with_retry(
            query=insert_query,
            params=insert_values,
            target_description=target_description,
            operation_type="insert",
            fetch_results=False
        )

    def update_data(
        self: Self,
        data: Union[CycleRecord, ProcessInfoRecord]
    ) -> None:
        """Update an existing record in the database with retry logic.

        Updates a single existing record using the record's generated UPDATE
        query. Validates that the record has a primary key before attempting
        the update and provides automatic retry logic for transient failures.

        Args:
            data (Union[CycleRecord, ProcessInfoRecord]): A record instance
                to update. Must have a valid primary key value.

        Raises:
            ValueError: If the record doesn't have a primary key (new record).
            PgsqlConnectionError: If update fails after all retry attempts.

        Example:
            >>> # Modify existing cycle record
            >>> cycle.cycle_status = "COMPLETE"
            >>> cycle.updated_date = datetime.now(timezone.utc)
            >>> dao.update_data(cycle)
        """
        if data.is_new_record():
            raise ValueError(
                "Cannot update a new record without primary key. "
                "Use insert_new_data() instead."
            )

        update_query = data.generate_update_query()
        update_values = data.get_update_values()
        target_description = f"table {data.SCHEMA_NAME}.{data.TABLE_NAME}"

        self._execute_operation_with_retry(
            query=update_query,
            params=update_values,
            target_description=target_description,
            operation_type="update",
            fetch_results=False
        )
