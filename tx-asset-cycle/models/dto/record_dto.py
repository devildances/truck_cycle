from abc import ABC
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any, ClassVar, List, Optional, Self, Tuple, Type, TypeVar

from psycopg2 import sql

from config.postgresql_config import (DX_SCHEMA_NAME,
                                      PGSQL_ASSET_CYCLE_TABLE_NAME,
                                      PGSQL_ASSET_CYCLE_TMP_TABLE_NAME,
                                      PGSQL_CHECKPOINT_TABLE_NAME)

T = TypeVar('T', bound='BaseRecord')
X = TypeVar('X', bound='CycleRecord')


@dataclass
class RealtimeRecord:
    """Real-time asset tracking data record.

    Represents real-time telemetry data from mining assets including
    location, speed, and work state information. This is typically used
    for incoming data streams before processing into cycle records.

    Attributes:
        asset_guid (str): Unique identifier for the asset.
        site_guid (str): Unique identifier for the mining site.
        timestamp (datetime): UTC timestamp when the data was recorded.
        speed (float): Current speed of the asset in appropriate units.
        work_state_id (int): Current work state identifier (e.g., 2=idle,
            3=working).
        longitude (float): GPS longitude coordinate in decimal degrees.
        latitude (float): GPS latitude coordinate in decimal degrees.

    Example:
        >>> record = RealtimeRecord(
        ...     asset_guid="truck-123",
        ...     site_guid="site-456",
        ...     timestamp=datetime.now(timezone.utc),
        ...     work_state_id=3,
        ...     longitude=-122.4194,
        ...     latitude=37.7749
        ... )
    """
    asset_guid: str
    site_guid: str
    timestamp: datetime
    work_state_id: int
    longitude: float
    latitude: float


@dataclass
class BaseRecord(ABC):
    """Abstract base class for database records with common functionality.

    Provides shared database operations for all record types including
    SQL query generation, data validation, and CRUD operation support.
    Uses psycopg2.sql for safe parameterized queries and automatic
    primary key handling.

    This base class implements the common patterns for:
    - Auto-incrementing primary key detection
    - Safe SQL query generation with proper escaping
    - Data tuple generation for batch operations
    - Update value preparation with primary key validation

    Subclasses must define:
        DB_PRIMARY_KEY_FIELD: Name of the auto-incrementing PK column
        TABLE_NAME: Database table name for this record type
        SCHEMA_NAME: Database schema name

    Example:
        >>> @dataclass
        ... class MyRecord(BaseRecord):
        ...     DB_PRIMARY_KEY_FIELD: ClassVar[str] = 'id'
        ...     TABLE_NAME: ClassVar[str] = 'my_table'
        ...     SCHEMA_NAME: ClassVar[str] = 'my_schema'
        ...
        ...     id: Optional[int] = None
        ...     name: str = ""
    """

    # These must be defined by subclasses
    DB_PRIMARY_KEY_FIELD: ClassVar[str]
    TABLE_NAME: ClassVar[str]
    SCHEMA_NAME: ClassVar[str]

    def is_new_record(self: Self) -> bool:
        """Check if this is a new record without a database primary key.

        Examines the primary key field to determine if this record has
        been persisted to the database. Used to distinguish between
        INSERT and UPDATE operations.

        Returns:
            bool: True if primary key is None (new record), False if
                primary key has a value (existing record).

        Example:
            >>> record = MyRecord(id=None, name="New")
            >>> print(record.is_new_record())  # True
            >>>
            >>> record.id = 42  # Simulate DB assignment
            >>> print(record.is_new_record())  # False
        """
        pk_value = getattr(self, self.DB_PRIMARY_KEY_FIELD, None)
        return pk_value is None

    @classmethod
    def get_column_names(cls: Type[T]) -> List[str]:
        """Get column names for database operations excluding primary key.

        Returns all dataclass field names except the primary key field
        and class variables (identified by uppercase names). Used for
        generating INSERT and UPDATE column lists.

        Returns:
            List[str]: Ordered list of column names suitable for SQL
                operations, excluding the auto-incrementing primary key.

        Note:
            Class variables (like TABLE_NAME) are excluded by checking
            for uppercase field names, following Python naming conventions.

        Example:
            >>> cols = MyRecord.get_column_names()
            >>> print(cols)  # ['name', 'created_date', ...]
        """
        return [
            f.name for f in fields(cls)
            if f.name != cls.DB_PRIMARY_KEY_FIELD
            and not f.name.isupper()  # Exclude class variables
        ]

    @classmethod
    def generate_insert_query(cls: Type[T]) -> sql.SQL:
        """Generate a safe parameterized INSERT query.

        Creates a PostgreSQL INSERT statement using psycopg2.sql for
        proper identifier escaping and parameterization. Excludes the
        primary key column to allow auto-generation by the database.

        Returns:
            sql.SQL: Parameterized INSERT query with schema, table, and
                column identifiers properly escaped. Ready for execution
                with parameter values.

        Example:
            >>> query = MyRecord.generate_insert_query()
            >>> print(query.as_string(connection))
            INSERT INTO "my_schema"."my_table" ("name", "created_date")
            VALUES (%s, %s);
        """
        columns = sql.SQL(", ").join(
            [sql.Identifier(col) for col in cls.get_column_names()]
        )
        placeholders = sql.SQL(", ").join(
            [sql.SQL("%s") for _ in cls.get_column_names()]
        )

        insert_query = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({placeholders});
            """
        ).format(
            schema=sql.Identifier(cls.SCHEMA_NAME),
            table=sql.Identifier(cls.TABLE_NAME),
            columns=columns,
            placeholders=placeholders
        )

        return insert_query

    def data_to_insert_tuple(self: Self) -> List[tuple]:
        """Convert record data to tuple format for batch INSERT operations.

        Extracts field values in the same order as get_column_names()
        and formats them as a list containing a single tuple. This format
        is compatible with psycopg2.extras.execute_values() for efficient
        batch insertions.

        Returns:
            List[tuple]: Single-element list containing a tuple of field
                values in column order, excluding the primary key.

        Note:
            Returns a list to maintain compatibility with batch operation
            patterns where multiple records would be processed together.

        Example:
            >>> record = MyRecord(id=None, name="Test", created_date=now)
            >>> data = record.data_to_insert_tuple()
            >>> print(data)  # [("Test", datetime(...),)]
        """
        ordered_fields = self.get_column_names()
        data_to_insert = []
        data_to_insert.append(
            tuple(getattr(self, field) for field in ordered_fields)
        )
        return data_to_insert

    @classmethod
    def generate_update_query(cls: Type[T]) -> sql.SQL:
        """Generate a safe parameterized UPDATE query.

        Creates a PostgreSQL UPDATE statement using psycopg2.sql for
        proper identifier escaping. Updates all non-primary key columns
        using the primary key in the WHERE clause for record identification.

        Returns:
            sql.SQL: Parameterized UPDATE query with SET clause for all
                columns and WHERE clause using primary key. Identifiers
                are properly escaped.

        Example:
            >>> query = MyRecord.generate_update_query()
            >>> print(query.as_string(connection))
            UPDATE "my_schema"."my_table"
            SET "name" = %s, "created_date" = %s
            WHERE "id" = %s;
        """
        columns = cls.get_column_names()
        set_clause = sql.SQL(", ").join(
            [sql.SQL("{col} = %s").format(col=sql.Identifier(col))
             for col in columns]
        )

        update_query = sql.SQL(
            """
            UPDATE {schema}.{table}
            SET {set_clause}
            WHERE {pk} = %s;
            """
        ).format(
            schema=sql.Identifier(cls.SCHEMA_NAME),
            table=sql.Identifier(cls.TABLE_NAME),
            set_clause=set_clause,
            pk=sql.Identifier(cls.DB_PRIMARY_KEY_FIELD)
        )

        return update_query

    def get_update_values(self: Self) -> Tuple[Any, ...]:
        """Get values for UPDATE query with primary key for WHERE clause.

        Extracts all non-primary key field values followed by the primary
        key value for the WHERE clause. Values are returned in the same
        order as the UPDATE query expects: SET clause values first, then
        the WHERE clause primary key value.

        Returns:
            Tuple[Any, ...]: Field values for UPDATE operation with primary
                key at the end. Order matches generate_update_query()
                parameter expectations.

        Raises:
            ValueError: If the primary key field is None, indicating this
                is a new record that cannot be updated.

        Example:
            >>> record = MyRecord(id=42, name="Updated", created_date=now)
            >>> values = record.get_update_values()
            >>> print(values)  # ("Updated", datetime(...), 42)
        """
        update_values = []
        db_pk_value = None

        for f in fields(self):
            value = getattr(self, f.name)
            if f.name == self.DB_PRIMARY_KEY_FIELD:
                db_pk_value = value
            elif not f.name.isupper():  # Exclude class variables
                update_values.append(value)

        if db_pk_value is None:
            raise ValueError(
                f"Cannot update record: Primary key {self.DB_PRIMARY_KEY_FIELD} is None"
            )

        return tuple(update_values + [db_pk_value])


@dataclass
class CycleRecord(BaseRecord):
    """Mining asset cycle tracking record.

    Represents a complete or in-progress operational cycle for a mining
    asset (typically trucks). Tracks the entire lifecycle from loading
    through dumping, including timing, locations, and performance metrics.

    A cycle typically consists of:
    1. Loading phase (at loader location)
    2. Loaded travel (to dump region)
    3. Dumping phase (in dump region)
    4. Empty travel (back to loader)

    Attributes:
        asset_cycle_vlx_id (Optional[int]): Auto-incrementing database
            primary key. None for new records.
        asset_guid (str): Unique identifier for the asset being tracked.
        cycle_number (int): Sequential cycle number for this asset.
        cycle_status (str): Current cycle status. Valid values:
            'INPROGRESS', 'COMPLETE', 'INVALID', 'OUTLIER'.
        site_guid (str): Unique identifier for the mining site.
        current_work_state_id (int): Current work state (2=idle, 3=working).
        created_date (datetime): UTC timestamp when record was created.
        updated_date (datetime): UTC timestamp of last update.

        # Optional timing and location fields
        current_segment (Optional[str]): Current phase of the cycle.
        previous_work_state_id (Optional[int]): Previous work state.
        loader_asset_guid (Optional[str]): Associated loader identifier.
        loader_latitude (Optional[float]): Loader GPS latitude.
        loader_longitude (Optional[float]): Loader GPS longitude.
        previous_loader_distance (Optional[float]): Previous distance to
            loader in meters.
        current_loader_distance (Optional[float]): Current distance to
            loader in meters.
        dump_region_guid (Optional[str]): Associated dump region identifier.
        is_outlier (Optional[bool]): Whether this cycle contains outlier data.
        outlier_position_latitude (Optional[float]): Outlier GPS latitude.
        outlier_position_longitude (Optional[float]): Outlier GPS longitude.

        # Timing fields (all in UTC)
        load_start_utc (Optional[datetime]): Loading phase start time.
        load_end_utc (Optional[datetime]): Loading phase end time.
        dump_start_utc (Optional[datetime]): Dumping phase start time.
        dump_end_utc (Optional[datetime]): Dumping phase end time.

        # Duration fields (all in seconds)
        load_travel_seconds (Optional[float]): Time traveling while loaded.
        empty_travel_seconds (Optional[float]): Time traveling while empty.
        dump_seconds (Optional[float]): Time spent dumping.
        load_seconds (Optional[float]): Time spent loading.
        total_cycle_seconds (Optional[float]): Total cycle duration.
        outlier_seconds (Optional[float]): Time spent in outlier state.

    Example:
        >>> cycle = CycleRecord(
        ...     asset_guid="truck-123",
        ...     cycle_number=1,
        ...     cycle_status="INPROGRESS",
        ...     site_guid="site-456",
        ...     current_work_state_id=3,
        ...     created_date=datetime.now(timezone.utc),
        ...     updated_date=datetime.now(timezone.utc)
        ... )
        >>> print(cycle.is_new_record())  # True
    """
    # Non-default fields
    asset_guid: str
    cycle_number: int
    cycle_status: str  # ["INPROGRESS", "COMPLETE", "INVALID", "OUTLIER"]
    site_guid: str
    current_work_state_id: int  # 3 if working, 2 if idling
    created_date: datetime
    updated_date: datetime

    # Class configuration
    DB_PRIMARY_KEY_FIELD: ClassVar[str] = 'asset_cycle_vlx_id'
    TABLE_NAME: ClassVar[str] = PGSQL_ASSET_CYCLE_TMP_TABLE_NAME
    FINAL_TABLE_NAME: ClassVar[str] = PGSQL_ASSET_CYCLE_TABLE_NAME
    SCHEMA_NAME: ClassVar[str] = DX_SCHEMA_NAME

    # Optional fields
    asset_cycle_vlx_id: Optional[int] = field(
        default=None,
        metadata={'db_primary_key': True}
    )
    current_segment: Optional[str] = None
    previous_work_state_id: Optional[int] = None  # 3 if working, 2 if idling
    loader_asset_guid: Optional[str] = None
    loader_latitude: Optional[float] = None
    loader_longitude: Optional[float] = None
    previous_loader_distance: Optional[float] = None
    current_loader_distance: Optional[float] = None
    dump_region_guid: Optional[str] = None
    is_outlier: Optional[bool] = None
    outlier_position_latitude: Optional[float] = None
    outlier_position_longitude: Optional[float] = None
    load_start_utc: Optional[datetime] = None
    load_end_utc: Optional[datetime] = None
    dump_start_utc: Optional[datetime] = None
    dump_end_utc: Optional[datetime] = None
    load_travel_seconds: Optional[float] = None
    empty_travel_seconds: Optional[float] = None
    dump_seconds: Optional[float] = None
    load_seconds: Optional[float] = None
    total_cycle_seconds: Optional[float] = None
    outlier_seconds: Optional[float] = None

    @classmethod
    def generate_insert_query_final(cls: Type[X]) -> sql.SQL:
        columns = sql.SQL(", ").join(
            [sql.Identifier(col) for col in cls.get_column_names()]
        )
        placeholders = sql.SQL(", ").join(
            [sql.SQL("%s") for _ in cls.get_column_names()]
        )

        insert_query = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({placeholders});
            """
        ).format(
            schema=sql.Identifier(cls.SCHEMA_NAME),
            table=sql.Identifier(cls.FINAL_TABLE_NAME),
            columns=columns,
            placeholders=placeholders
        )

        return insert_query


@dataclass
class ProcessInfoRecord(BaseRecord):
    """Process execution tracking and checkpoint record.

    Tracks the execution state and progress of data processing operations
    for mining assets. Used as a checkpoint mechanism to prevent
    reprocessing of data and maintain processing state across system
    restarts or failures.

    This record type is essential for:
    - Tracking the last processed timestamp per asset
    - Preventing duplicate data processing
    - Maintaining processing state for recovery
    - Auditing processing history and performance

    Attributes:
        tx_process_info_id (Optional[int]): Auto-incrementing database
            primary key. None for new records.
        asset_guid (str): Unique identifier for the asset being processed.
        site_guid (str): Unique identifier for the mining site.
        process_name (str): Name of the processing operation (e.g.,
            'asset_cycle_processing').
        process_date (datetime): UTC timestamp of the data being processed.
        created_date (datetime): UTC timestamp when record was created.
        updated_date (Optional[datetime]): UTC timestamp of last update,
            None for new records.

    Example:
        >>> process_info = ProcessInfoRecord(
        ...     asset_guid="truck-123",
        ...     site_guid="site-456",
        ...     process_name="asset_cycle_processing",
        ...     process_date=datetime.now(timezone.utc),
        ...     created_date=datetime.now(timezone.utc)
        ... )
        >>> print(process_info.is_new_record())  # True
    """
    # Non-default fields
    asset_guid: str
    site_guid: str
    process_name: str
    process_date: datetime
    created_date: datetime

    # Class configuration
    DB_PRIMARY_KEY_FIELD: ClassVar[str] = 'tx_process_info_id'
    TABLE_NAME: ClassVar[str] = PGSQL_CHECKPOINT_TABLE_NAME
    SCHEMA_NAME: ClassVar[str] = DX_SCHEMA_NAME

    # Optional fields
    tx_process_info_id: Optional[int] = field(
        default=None,
        metadata={'db_primary_key': True}
    )
    updated_date: Optional[datetime] = None
