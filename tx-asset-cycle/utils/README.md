# Utilities and Helper Functions

This folder contains utility functions, helper scripts, and operational tools for the mining asset tracking system. These components provide essential support for data processing, system monitoring, AWS Kinesis integration, and mathematical calculations.

## File Overview

```
utils/
├── utilities.py      # Core utility functions (datetime, geospatial, AWS, error handling)
├── checkpoint.py     # Processing state management (in-memory cache)
├── healthcheck.py    # System health monitoring & dependency verification
└── kclpy_helper.py   # AWS Kinesis Consumer Library (KCL) integration
```

## Core Components

### `utilities.py` - Core Utility Functions
Essential utilities for data processing and system operations:

- **Error Handling**: Stack trace extraction and formatting
- **DateTime Processing**: ISO timestamp parsing, timezone conversion, duration calculations
- **Geospatial Operations**: GPS coordinate validation, Haversine distance calculations
- **AWS Integration**: Secure credential retrieval from Secrets Manager
- **Validation Utilities**: Input validation for coordinates and thresholds

### `checkpoint.py` - Processing State Management
Simple in-memory cache for tracking processing checkpoints:

```python
# Global state for preventing duplicate processing
LATEST_PROCESS_INFO: dict = None
```

Used to cache the latest processed timestamps per asset to avoid reprocessing data.

### `healthcheck.py` - System Health Monitoring
Comprehensive health check script that validates:

- Python module imports
- AWS connectivity and permissions
- PostgreSQL database connectivity
- Redis cache connectivity (optional)
- Kinesis stream access and status

Returns structured JSON output suitable for container health checks and monitoring systems.

### `kclpy_helper.py` - Kinesis Integration
Wrapper script that manages the Java KCL daemon for Python consumers:

- Handles Java classpath configuration across platforms
- Manages KCL daemon lifecycle and graceful shutdown
- Monitors memory usage of both Python and Java processes
- Supports environment-based configuration (realtime vs reprocess)

## Usage Patterns

### Basic Utility Usage
```python
from utils.utilities import haversine_distance, validate_coordinates

# Geospatial calculations
truck_pos = [37.7749, -122.4194]
loader_pos = [37.7849, -122.4094]
distance = haversine_distance(truck_pos, loader_pos)
```

### Processing State Management
```python
from utils import checkpoint

# Check if record already processed
if checkpoint.LATEST_PROCESS_INFO and asset_guid in checkpoint.LATEST_PROCESS_INFO:
    # Skip already processed records
    return

# Update checkpoint after processing
checkpoint.LATEST_PROCESS_INFO[asset_guid] = current_timestamp.isoformat()
```

### Health Monitoring
```bash
# Container health check
python3 /app/utils/healthcheck.py

# Returns JSON with health status
# Exit code 0 = healthy, 1 = unhealthy
```

### KCL Consumer Launch
```bash
# Environment setup
export TX_TYPE=realtime
export KINESIS_STREAM_NAME=mining-asset-data

# Launch consumer
python3 /app/utils/kclpy_helper.py
```

## Integration with Main Application

### Error Handling
```python
from utils.utilities import get_stack_trace_py

try:
    process_mining_data(data)
except Exception as e:
    logger.error(f"Processing failed: {get_stack_trace_py(e)}")
```

### AWS Credentials
```python
from utils.utilities import get_db_credentials_from_secrets_manager

pg_creds = get_db_credentials_from_secrets_manager(
    "mining-db-prod", "us-west-2", "pgsql"
)
```

### Distance Calculations
```python
from utils.utilities import haversine_distance

# Check if truck is near loader
distance = haversine_distance(truck_position, loader_position)
if distance <= PROXIMITY_THRESHOLD:
    start_loading_sequence()
```

## Operational Usage

### Docker Health Checks
```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python3 /app/utils/healthcheck.py
```

### Kubernetes Probes
```yaml
livenessProbe:
  exec:
    command: ["python3", "/app/utils/healthcheck.py"]
  periodSeconds: 60

readinessProbe:
  exec:
    command: ["python3", "/app/utils/healthcheck.py"]
  periodSeconds: 30
```

### Environment Configuration
```bash
# Required for KCL helper
TX_TYPE=realtime|reprocess
KINESIS_STREAM_NAME=stream-name

# Optional JVM tuning
JAVA_OPTS="-Xms512m -Xmx768m -XX:+UseG1GC"

# AWS configuration
AWS_REGION=us-west-2
```

## Testing

### Unit Tests
```python
def test_distance_calculation():
    # Test known distance between cities
    sf = [37.7749, -122.4194]
    la = [34.0522, -118.2437]
    distance_km = haversine_distance(sf, la) / 1000
    assert 554 <= distance_km <= 564  # ~559km actual

def test_coordinate_validation():
    with pytest.raises(ValueError):
        validate_coordinates([95.0, -122.0])  # Invalid latitude
```

### Health Check Testing
```bash
# Test health check script
python3 utils/healthcheck.py
echo $?  # Should be 0 for healthy system
```

## Performance Considerations

- **Coordinate calculations**: Optimized Haversine implementation for real-time processing
- **Memory monitoring**: Tracks both Python and Java process memory usage
- **Checkpoint caching**: In-memory cache prevents redundant database queries
- **Health checks**: Designed for frequent execution with minimal overhead

## Best Practices

### Error Handling
```python
# ✅ Always preserve stack traces
logger.error(f"Failed: {get_stack_trace_py(e)}")

# ❌ Don't lose error context
logger.error("Something failed")
```

### Coordinate Validation
```python
# ✅ Validate before calculations
validate_coordinates(point1)
validate_coordinates(point2)
distance = haversine_distance(point1, point2)

# ❌ Skip validation at your own risk
distance = haversine_distance(point1, point2)
```

### Resource Management
```python
# ✅ Use utilities for consistent behavior
from utils.utilities import format_log_elapse_time

elapsed = format_log_elapse_time(duration)
logger.info(f"Completed in {elapsed}")
```

## Contributing

When adding new utilities:

1. **Keep it focused**: Utils should be reusable across the application
2. **Add validation**: Include proper input validation and error handling
3. **Document thoroughly**: Add comprehensive docstrings to the functions themselves
4. **Test coverage**: Write unit tests for new utilities
5. **Performance aware**: Consider impact on high-frequency operations

For detailed function documentation, refer to the docstrings within each file.