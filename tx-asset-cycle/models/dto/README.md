# Data Transfer Objects (DTO) Layer

This folder contains the Data Transfer Object (DTO) layer for the mining asset tracking system, providing structured data representations for all entities in the system. DTOs serve as the contract between different layers of the application and ensure type safety throughout the data processing pipeline.

## Architecture Overview

The DTO layer is organized into four main categories representing different aspects of the mining operation:

```
DTO Layer Structure
├── asset_dto.py          # Mining equipment representations
├── record_dto.py         # Database records & real-time data
├── region_polygon_dto.py # Geographical regions & spatial operations
└── method_dto.py         # Processing context & method parameters
```

## Core Components

### Asset DTOs (`asset_dto.py`)

#### `LoaderAsset`
Represents heavy equipment used for loading operations in mining sites.

```python
@dataclass
class LoaderAsset:
    asset_guid: str      # Unique asset identifier
    site_guid: str       # Associated mining site
    latitude: float      # GPS latitude coordinate
    longitude: float     # GPS longitude coordinate
```

**Use Cases:**

- Proximity calculations for truck-loader interactions
- Spatial analysis for loading zone optimization
- Asset tracking and positioning

#### `TruckAsset`
Represents mining trucks that transport materials between locations.

```python
@dataclass
class TruckAsset:
    asset_guid: str           # Unique asset identifier
    site_guid: str            # Associated mining site
    asset_type_guid: str      # Type classification (e.g., haul truck, water truck)
```

**Use Cases:**

- Asset type validation for cycle processing
- Fleet management and categorization
- Operational workflow routing

### Record DTOs (`record_dto.py`)

#### `RealtimeRecord`
Real-time telemetry data from mining assets, representing the raw input stream.

```python
@dataclass
class RealtimeRecord:
    asset_guid: str        # Asset identifier
    site_guid: str         # Mining site identifier
    timestamp: datetime    # UTC timestamp of data point
    work_state_id: int     # Work state (2=idle, 3=working)
    longitude: float       # GPS longitude coordinate
    latitude: float        # GPS latitude coordinate
```

**Use Cases:**

- Input for real-time processing pipeline
- Cycle detection and state transitions
- Performance monitoring and analytics

#### `BaseRecord` (Abstract)
Abstract base class providing common database operations for all record types.

**Key Features:**

- Auto-incrementing primary key management
- Safe SQL query generation with psycopg2.sql
- INSERT/UPDATE operation support
- Batch operation compatibility

**Database Operations:**
```python
# Common methods for all record types
def is_new_record() -> bool
def get_column_names() -> List[str]
def generate_insert_query() -> sql.SQL
def generate_update_query() -> sql.SQL
def get_update_values() -> Tuple[Any, ...]
```

#### `CycleRecord`
Comprehensive tracking record for complete mining asset operational cycles.

**Lifecycle Phases:**

1. **Loading Phase**: Asset positioned at loader, material loading
2. **Loaded Travel**: Asset transporting material to dump location
3. **Dumping Phase**: Asset unloading material at dump site
4. **Empty Travel**: Asset returning to loader for next cycle

**Key Attributes:**
```python
# Core cycle identification
asset_guid: str
cycle_number: int
cycle_status: str          # INPROGRESS, COMPLETE, INVALID, OUTLIER
site_guid: str

# Timing tracking (all UTC)
load_start_utc: Optional[datetime]
load_end_utc: Optional[datetime]
dump_start_utc: Optional[datetime]
dump_end_utc: Optional[datetime]

# Performance metrics (seconds)
load_travel_seconds: Optional[int]
empty_travel_seconds: Optional[int]
dump_seconds: Optional[int]
load_seconds: Optional[int]
total_cycle_seconds: Optional[int]

# Location tracking
loader_asset_guid: Optional[str]
loader_latitude: Optional[float]
loader_longitude: Optional[float]
dump_region_guid: Optional[str]
```

**Database Support:**

- Primary table: Temporary processing table
- Final table: Archive table for completed cycles
- Dual insert support via `generate_insert_query_final()`

#### `ProcessInfoRecord`
Checkpoint record for tracking processing state and preventing data reprocessing.

```python
@dataclass
class ProcessInfoRecord:
    asset_guid: str         # Asset being processed
    site_guid: str          # Associated site
    process_name: str       # Processing operation name
    process_date: datetime  # Timestamp of data being processed
    created_date: datetime  # Record creation time
```

**Use Cases:**

- Exactly-once processing guarantees
- Recovery from system failures
- Processing progress tracking
- Audit trail for data operations

### Geographical DTOs (`region_polygon_dto.py`)

#### `RegionPoly`
Sophisticated geographical region representation with spatial operations.

**Core Features:**

- Coordinate string parsing and validation
- Shapely polygon integration for spatial operations
- Caching for performance optimization
- Comprehensive error handling and logging

**Coordinate Format:**
```
"longitude1 latitude1, longitude2 latitude2, longitude3 latitude3, ..."
Example: "-122.4194 37.7749, -122.4094 37.7849, -122.4294 37.7849"
```

**Spatial Operations:**
```python
# Get parsed polygon for spatial calculations
polygon = region.get_polygon()

# Point-in-polygon testing
from shapely.geometry import Point
truck_point = Point(longitude, latitude)
is_inside = polygon.contains(truck_point)

# Area calculations
area_sq_meters = polygon.area
```

**Region Types:**

- **Dump Zones**: Areas designated for material dumping
- **Load Zones**: Areas where loading operations occur
- **Restricted Areas**: No-access zones for safety or environmental reasons

### Method DTOs (`method_dto.py`)

#### `CycleComparisonContext`
Context object for cycle state comparison and decision making.

```python
@dataclass
class CycleComparisonContext:
    current_record: RealtimeRecord        # Current telemetry data
    last_record: CycleRecord             # Previous cycle state
    asset_position: str                  # Current position context
    loader_asset: Optional[LoaderAsset]  # Nearby loader if any
    loader_distance: Optional[float]     # Distance to loader (meters)
    dump_region: Optional[RegionPoly]    # Current dump region if any
```

**Use Cases:**
- State transition decision making
- Cycle progression analysis
- Context-aware processing logic
- Performance optimization based on asset position

## Usage Patterns

### Real-time Data Processing
```python
from models.dto.record_dto import RealtimeRecord, CycleRecord
from models.dto.asset_dto import LoaderAsset
from models.dto.region_polygon_dto import RegionPoly

# Process incoming telemetry
realtime_data = RealtimeRecord(
    asset_guid="truck-123",
    site_guid="site-456",
    timestamp=datetime.now(timezone.utc),
    work_state_id=3,
    longitude=-122.4194,
    latitude=37.7749
)

# Create new cycle if needed
if is_new_cycle_needed(realtime_data):
    cycle = CycleRecord(
        asset_guid=realtime_data.asset_guid,
        cycle_number=get_next_cycle_number(),
        cycle_status="INPROGRESS",
        site_guid=realtime_data.site_guid,
        current_work_state_id=realtime_data.work_state_id,
        created_date=datetime.now(timezone.utc),
        updated_date=datetime.now(timezone.utc)
    )
```

### Spatial Analysis
```python
from models.dto.region_polygon_dto import RegionPoly
from shapely.geometry import Point

# Define operational zones
dump_zone = RegionPoly(
    region_guid="dump-001",
    site_guid="site-123",
    name="Primary Dump Zone",
    region_points="-122.4 37.7, -122.3 37.7, -122.35 37.8",
    dump_site=True,
    region_type="DUMP_ZONE"
)

# Check asset position
truck_position = Point(-122.35, 37.75)
dump_polygon = dump_zone.get_polygon()

if dump_polygon and dump_polygon.contains(truck_position):
    print("Truck is in dump zone - start dump timing")
```

### Database Operations
```python
from models.dto.record_dto import CycleRecord
from models.dao.pgsql_action_dao import PgsqlActionDAO

# Create and persist cycle record
cycle = CycleRecord(
    asset_guid="truck-123",
    cycle_number=1,
    cycle_status="INPROGRESS",
    # ... other fields
)

with PgsqlActionDAO() as dao:
    # Insert new cycle
    if cycle.is_new_record():
        dao.insert_new_data(cycle)

    # Update existing cycle
    cycle.cycle_status = "COMPLETE"
    cycle.updated_date = datetime.now(timezone.utc)
    dao.update_data(cycle)

    # Move to final table when complete
    if cycle.cycle_status == "COMPLETE":
        dao.insert_new_final_cycle_data(cycle)
```

### Context-driven Processing
```python
from models.dto.method_dto import CycleComparisonContext

# Build processing context
context = CycleComparisonContext(
    current_record=realtime_data,
    last_record=current_cycle,
    asset_position="NEAR_LOADER",
    loader_asset=nearest_loader,
    loader_distance=15.5,
    dump_region=None
)

# Make processing decisions based on context
if context.loader_distance and context.loader_distance < 20.0:
    # Asset is near loader - check for loading activity
    if context.current_record.work_state_id == 3:  # Working
        start_loading_phase(context)
```

## Data Flow Architecture

### Processing Pipeline
```
Raw Telemetry → RealtimeRecord → Processing → CycleRecord → Database
                      ↓
                CycleComparisonContext
                      ↓
              Spatial Analysis (RegionPoly)
                      ↓
              Asset Proximity (LoaderAsset)
                      ↓
              State Decisions & Updates
```

### Database Persistence
```
RealtimeRecord (Input) → Processing Logic → CycleRecord (Output)
                                               ↓
                                         Temporary Table
                                               ↓
                                    (When Complete) → Final Table
```

## Validation and Error Handling

### Coordinate Validation
```python
# RegionPoly automatically validates coordinates
region = RegionPoly(
    region_guid="test-region",
    region_points="invalid-format"
)

polygon = region.get_polygon()
if polygon is None:
    print(f"Parsing failed: {region._parse_error_message}")
```

### Database Record Validation
```python
# BaseRecord validates primary key state
cycle = CycleRecord(...)

# Check if record is new or existing
if cycle.is_new_record():
    dao.insert_new_data(cycle)
else:
    dao.update_data(cycle)
```

### Type Safety
All DTOs use Python dataclasses with type hints for:

- IDE autocomplete and error detection
- Runtime type validation
- Clear API contracts
- Documentation generation

## Performance Considerations

### Caching Strategies
- **RegionPoly**: Caches parsed Shapely polygons to avoid recomputation
- **BaseRecord**: Reuses query objects for repeated operations
- **Spatial Operations**: Minimizes object creation in hot paths

### Memory Management
- **Dataclass Efficiency**: Lightweight object creation
- **Optional Fields**: Reduces memory footprint for sparse data
- **Lazy Loading**: RegionPoly parses coordinates only when needed

### Batch Operations
```python
# Efficient batch processing
cycles = [cycle1, cycle2, cycle3]
insert_values = [cycle.data_to_insert_tuple()[0] for cycle in cycles]

# Single database round trip for multiple records
dao._execute_operation_with_retry(
    query=CycleRecord.generate_insert_query(),
    params=insert_values,
    operation_type="insert"
)
```

## Testing Strategies

### Unit Testing DTOs
```python
def test_cycle_record_lifecycle():
    """Test cycle record from creation to completion."""
    # Create new cycle
    cycle = CycleRecord(
        asset_guid="test-truck",
        cycle_number=1,
        cycle_status="INPROGRESS",
        # ... required fields
    )

    assert cycle.is_new_record()

    # Simulate database assignment
    cycle.asset_cycle_vlx_id = 123
    assert not cycle.is_new_record()

    # Test update values
    update_values = cycle.get_update_values()
    assert update_values[-1] == 123  # PK at end

def test_region_polygon_parsing():
    """Test coordinate parsing and validation."""
    region = RegionPoly(
        region_guid="test",
        region_points="-122.4 37.7, -122.3 37.7, -122.35 37.8"
    )

    polygon = region.get_polygon()
    assert polygon is not None
    assert polygon.is_valid
```

### Integration Testing
```python
def test_spatial_operations_integration():
    """Test real-world spatial calculations."""
    # Create dump region
    dump_zone = RegionPoly(
        region_guid="dump-001",
        region_points="known-valid-coordinates"
    )

    # Test truck positions
    truck_positions = [
        (-122.35, 37.75),  # Inside
        (-122.50, 37.50),  # Outside
    ]

    polygon = dump_zone.get_polygon()
    for lon, lat in truck_positions:
        point = Point(lon, lat)
        result = polygon.contains(point)
        # Assert based on known geometry
```

## Migration and Versioning

### Adding New Fields
```python
# Safe field addition with defaults
@dataclass
class CycleRecord(BaseRecord):
    # Existing fields...

    # New optional field
    fuel_consumption: Optional[float] = None

    # New required field with default
    cycle_version: str = "2.0"
```

### Schema Evolution
1. **Add optional fields** with defaults to maintain compatibility
2. **Update database schema** before deploying new DTO versions
3. **Test backward compatibility** with existing data
4. **Document breaking changes** in migration guides

## Best Practices

### DTO Design
```python
# ✅ Use clear, descriptive field names
@dataclass
class AssetPosition:
    asset_guid: str
    latitude: float
    longitude: float
    timestamp: datetime

# ❌ Avoid ambiguous or abbreviated names
@dataclass
class AssetPos:
    id: str
    lat: float
    lng: float
    ts: datetime
```

### Optional vs Required Fields
```python
# ✅ Make core business fields required
@dataclass
class CycleRecord:
    asset_guid: str          # Required - core identifier
    cycle_status: str        # Required - business critical

    # Optional - may not be available initially
    load_start_utc: Optional[datetime] = None
    dump_region_guid: Optional[str] = None

# ❌ Don't make everything optional
@dataclass
class BadRecord:
    asset_guid: Optional[str] = None  # Should be required
    cycle_status: Optional[str] = None  # Should be required
```

### Error Handling
```python
# ✅ Provide meaningful error messages
def validate_coordinates(self) -> bool:
    if self.latitude < -90 or self.latitude > 90:
        raise ValueError(
            f"Invalid latitude {self.latitude}: must be between -90 and 90"
        )

# ❌ Don't fail silently
def validate_coordinates(self) -> bool:
    if self.latitude < -90 or self.latitude > 90:
        return False  # Caller doesn't know what's wrong
```

### Documentation
```python
# ✅ Document field meanings and constraints
@dataclass
class LoaderAsset:
    """Heavy equipment used for loading operations.

    Attributes:
        asset_guid: Unique identifier, format: "loader-{site}-{number}"
        latitude: GPS latitude in decimal degrees (-90 to 90)
        longitude: GPS longitude in decimal degrees (-180 to 180)
    """
    asset_guid: str
    latitude: float  # Range: -90.0 to 90.0
    longitude: float  # Range: -180.0 to 180.0
```

## Contributing

### Adding New DTOs
1. **Follow naming conventions**: `*_dto.py` for files, clear class names
2. **Use dataclasses**: Consistent with existing patterns
3. **Add type hints**: Complete type annotations for all fields
4. **Document thoroughly**: Comprehensive docstrings following E501
5. **Include examples**: Usage examples in docstrings
6. **Write tests**: Unit tests for validation and core functionality

### Modifying Existing DTOs
1. **Maintain backward compatibility**: Add optional fields with defaults
2. **Update related code**: DAO operations, processing logic
3. **Test thoroughly**: Ensure no breaking changes
4. **Update documentation**: Keep examples and descriptions current
5. **Consider migration**: Document any required data migrations