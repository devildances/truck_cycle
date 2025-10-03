# Data Access Object (DAO) Layer

This folder contains the Data Access Object (DAO) layer for the mining asset tracking system, providing a clean abstraction layer between the business logic and data persistence systems (PostgreSQL and Redis).

## Architecture Overview

The DAO layer follows a hierarchical inheritance pattern with base classes providing common functionality and specialized classes implementing specific operations:

```
Base Layer (Connection Management)
├── PgsqlBaseDAO           # PostgreSQL connection & transaction management
└── RedisBaseDAO           # Redis connection & health monitoring

Action Layer (Query Execution)
├── PgsqlActionDAO         # PostgreSQL CRUD operations & query execution
└── RedisSourceDAO         # Redis data retrieval & caching operations
```

## Core Components

### Base DAOs

#### `PgsqlBaseDAO`
- **Purpose**: PostgreSQL connection management and transaction control
- **Features**:
    - Automatic connection retry with exponential backoff
    - Context manager support for resource cleanup
    - Transaction management (commit/rollback)
    - Connection pooling and health monitoring

#### `RedisBaseDAO`
- **Purpose**: Redis connection management and health checking
- **Features**:
    - SSL/TLS connection support
    - Automatic reconnection with ping health checks
    - Context manager support
    - Configurable timeouts and retry logic

### Action DAOs

#### `PgsqlActionDAO`
- **Purpose**: High-level PostgreSQL operations with security and retry logic
- **Features**:
    - Table access validation and whitelisting
    - SQL injection protection via parameterized queries
    - Unified retry logic for all database operations
    - Support for custom SQL and table-based queries
    - Batch operations with automatic optimization

#### `RedisSourceDAO`
- **Purpose**: Redis data retrieval with structured data handling
- **Features**:
    - Flexible key pattern support
    - Multiple data structure types (string, set, hash)
    - Region data processing for mining operations
    - Version filtering and data transformation

## Usage Patterns

### PostgreSQL Operations

#### Basic Query Operations
```python
from models.dao.pgsql_action_dao import PgsqlActionDAO

# Using context manager (recommended)
with PgsqlActionDAO() as dao:
    # Table-based query
    result = dao.pull_data_from_table(
        single_query={
            'table': 'asset_cycles',
            'target_columns': ['asset_guid', 'cycle_status'],
            'target_filter': "cycle_status = 'INPROGRESS'",
            'target_limit': 100,
            'target_order': 'created_date DESC'
        }
    )

    # Custom SQL query
    custom_result = dao.pull_data_from_table(
        custom_query={
            'query': sql.SQL(
                "SELECT COUNT(*) as total FROM {schema}.{table} WHERE status = %s"
            ).format(
                schema=sql.Identifier("dx"),
                table=sql.Identifier("asset_cycles")
            ),
            'params': ['COMPLETE']
        }
    )
```

#### Record Operations
```python
from models.dto.record_dto import CycleRecord
from datetime import datetime, timezone

# Insert new record
cycle = CycleRecord(
    asset_guid="truck-123",
    cycle_status="INPROGRESS",
    site_guid="site-456",
    current_work_state_id=3,
    created_date=datetime.now(timezone.utc),
    updated_date=datetime.now(timezone.utc)
)

with PgsqlActionDAO() as dao:
    # Insert new record
    dao.insert_new_data(cycle)

    # Update existing record (after setting primary key)
    cycle.cycle_status = "COMPLETE"
    cycle.updated_date = datetime.now(timezone.utc)
    dao.update_data(cycle)

    # Insert to final table (for completed cycles)
    dao.insert_new_final_cycle_data(cycle)
```

### Redis Operations

#### Basic Data Retrieval
```python
from models.dao.redis_source_dao import RedisSourceDAO, RedisKeyPattern

with RedisSourceDAO() as dao:
    # Get asset regions (latest versions only)
    regions = dao.pull_region_based_asset("truck-123")

    # Get all region versions
    all_regions = dao.get_asset_regions(
        "truck-123",
        include_all_versions=True
    )

    # Flexible key pattern access
    pattern = RedisKeyPattern(
        "asset:{asset_id}:status",
        {"asset_id": "truck-123"}
    )
    status = dao.get_data_by_key_pattern(pattern, "string")
```

## Error Handling

### Exception Hierarchy
```
Exception
├── PgsqlConnectionError      # PostgreSQL connection issues
├── RedisConnectionError      # Redis connection issues
└── DataAccessError          # General data access failures
```

### Retry Logic
- **PostgreSQL**: 2 retry attempts with automatic rollback
- **Redis**: 3 retry attempts with exponential backoff
- **Transient errors**: Automatic retry with detailed logging
- **Non-retryable errors**: Immediate failure with context

### Example Error Handling
```python
from models.dao.pgsql_action_dao import PgsqlActionDAO, PgsqlConnectionError

try:
    with PgsqlActionDAO() as dao:
        result = dao.pull_data_from_table(single_query={'table': 'users'})
except PgsqlConnectionError as e:
    logger.error(f"Database connection failed: {e}")
    logger.error(f"Original exception: {e.original_exception}")
    logger.error(f"Connection details: {e.connection_details}")
```

## Security Features

### SQL Injection Protection
- **Parameterized queries**: All user inputs are properly escaped
- **psycopg2.sql composition**: Safe identifier and literal handling
- **Table whitelisting**: Only approved tables can be accessed

### Access Control
```python
# Only tables in ALLOWED_TABLES can be queried
ALLOWED_TABLES = [
    'asset_cycles',
    'process_checkpoints',
    'asset_master'
]
```

## Performance Optimization

### PostgreSQL Optimizations
- **Batch operations**: Automatic `execute_values()` for multiple records
- **Connection pooling**: Efficient connection reuse
- **Query optimization**: Prepared statements and proper indexing

### Redis Optimizations
- **Connection reuse**: Persistent connections with health monitoring
- **Pipeline operations**: Efficient multi-command execution
- **Data filtering**: Server-side filtering to reduce network traffic

## Configuration

### Environment Variables
```bash
# PostgreSQL Configuration
PGSQL_HOST=localhost
PGSQL_PORT=5432
PGSQL_DATABASE=mining_db
PGSQL_USERNAME=app_user
PGSQL_PASSWORD=secure_password

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_USERNAME=redis_user
REDIS_PASSWORD=redis_password
```

### Configuration Files
- `config/postgresql_config.py`: PostgreSQL settings and table whitelist
- `config/redis_config.py`: Redis connection parameters

## Testing

### Unit Tests
```python
import pytest
from models.dao.pgsql_action_dao import PgsqlActionDAO

class TestPgsqlActionDAO:
    def test_connection_retry_logic(self):
        # Test retry behavior with mock failures
        pass

    def test_table_access_validation(self):
        # Test whitelist enforcement
        pass

    def test_sql_injection_protection(self):
        # Test parameterized query safety
        pass
```

### Integration Tests
```python
class TestDatabaseIntegration:
    def test_end_to_end_cycle_operations(self):
        # Test complete CRUD cycle
        with PgsqlActionDAO() as dao:
            # Insert -> Query -> Update -> Delete
            pass
```

## Best Practices

### Connection Management
```python
# ✅ Always use context managers
with PgsqlActionDAO() as dao:
    dao.insert_new_data(record)

# ❌ Avoid manual connection management
dao = PgsqlActionDAO()
try:
    dao.insert_new_data(record)
finally:
    dao.close_connection()  # Easy to forget
```

### Error Handling
```python
# ✅ Handle specific exceptions
try:
    with PgsqlActionDAO() as dao:
        result = dao.pull_data_from_table(query)
except PgsqlConnectionError as e:
    # Handle connection issues
    logger.error(f"Database unavailable: {e}")
except ValueError as e:
    # Handle validation errors
    logger.error(f"Invalid data: {e}")

# ❌ Don't catch all exceptions
except Exception as e:
    # Too broad, masks specific issues
    pass
```

### Query Construction
```python
# ✅ Use parameterized queries
query = sql.SQL("SELECT * FROM {table} WHERE status = %s").format(
    table=sql.Identifier("asset_cycles")
)
params = ["ACTIVE"]

# ❌ Never use string formatting for user input
query = f"SELECT * FROM asset_cycles WHERE status = '{user_input}'"  # SQL injection risk
```

### Transaction Management
```python
# ✅ Use transactions for related operations
with PgsqlActionDAO() as dao:
    try:
        dao.insert_new_data(cycle_record)
        dao.insert_new_data(process_record)
        dao.commit()
    except Exception:
        dao.rollback()
        raise

# ❌ Don't forget transaction boundaries
dao.insert_new_data(record1)
dao.insert_new_data(record2)  # No guarantee both succeed
```

## Monitoring and Logging

### Log Levels
- **INFO**: Successful operations, connection events
- **WARNING**: Retry attempts, non-critical issues
- **ERROR**: Operation failures, connection errors

### Metrics to Monitor
- Connection pool utilization
- Query execution times
- Retry attempt frequency
- Error rates by operation type

## Migration and Deployment

### Database Schema Changes
1. Update table whitelist in `postgresql_config.py`
2. Update DTO classes to match new schema
3. Test DAO operations with new schema
4. Deploy with backward compatibility

### Version Compatibility
- **PostgreSQL**: Supports versions 12+
- **Redis**: Supports versions 6+
- **Python**: Requires 3.11+ for `Self` type hints

## Troubleshooting

### Common Issues

#### Connection Timeouts
```python
# Increase timeout in configuration
REDIS_SOCKET_TIMEOUT = 10  # Default: 5 seconds
PGSQL_CONNECT_TIMEOUT = 30  # Default: 15 seconds
```

#### Table Access Denied
```python
# Add table to whitelist
ALLOWED_TABLES = [
    'existing_table',
    'new_table_name'  # Add here
]
```

#### Performance Issues
- Check connection pool size
- Monitor query execution plans
- Verify proper indexing
- Consider read replicas for heavy queries

## Contributing

1. **Follow naming conventions**: `*_dao.py` for DAO files
2. **Add comprehensive docstrings**: Follow existing E501 compliance
3. **Include error handling**: Proper exception handling and logging
4. **Write tests**: Unit and integration tests for new functionality
5. **Update documentation**: Keep README.md current with changes