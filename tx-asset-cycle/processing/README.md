# Processing Pipeline

This folder contains the core data processing pipeline for the mining asset cycle tracking service. The pipeline transforms raw telemetry data from AWS Kinesis into structured cycle records, implementing the Extract-Transform-Load (ETL) pattern optimized for real-time streaming data.

## Pipeline Architecture

```
AWS Kinesis Stream → Extract → Transform → Load → PostgreSQL
                       ↓         ↓         ↓
                   Raw JSON → Business → Database
                   Records    Logic     Writes
```

## Processing Flow

The pipeline follows a three-phase approach:

1. **Extract Phase**: Consumes and validates raw telemetry from Kinesis
2. **Transform Phase**: Enriches data and applies business logic
3. **Load Phase**: Persists processed data to PostgreSQL

## File Structure

```
processing/
├── extract_phase.py           # Kinesis Consumer Library (KCL) integration
├── transform_realtime_phase.py # Data enrichment and business logic
├── load_realtime_phase.py     # Database persistence layer
├── preprocess.py              # Utility functions for data processing
└── cores/                     # Business logic engine
    ├── identifiers.py         # Enums, constants, and factory classes
    ├── segment_decision.py    # Cycle state management engine
    └── code_logic_flow.md     # Detailed business logic documentation
```

## Phase Details

### Extract Phase (`extract_phase.py`)

**Purpose**: AWS Kinesis Consumer Library (KCL) integration and raw data consumption

**Key Responsibilities**:

- Implements KCL `RecordProcessorBase` interface
- Manages shard lifecycle (initialize, process, shutdown)
- Handles checkpointing for exactly-once processing
- Routes records through the processing pipeline
- Monitors memory usage of Python and Java processes

**Class**: `RecordProcessor`

**Key Methods**:

- `initialize()`: Set up database connections when assigned to shard
- `process_records()`: Batch process records from Kinesis
- `checkpoint()`: Save processing progress with retry logic
- `process_record()`: Route individual record through transform→load pipeline

**Error Handling**:

- Automatic retry logic for transient failures
- Graceful degradation on processing errors
- Comprehensive logging for debugging and monitoring

**Example Flow**:
```python
# Called by KCL framework
def process_records(self, process_records_input):
    for record in process_records_input.records:
        # Decode and parse JSON
        data = json.loads(record.binary_data.decode('utf-8'))

        # Route through pipeline
        self.process_record(data, record.partition_key, ...)

        # Track sequence for checkpointing
        self.update_sequence_tracking(record)

    # Periodic checkpoint
    if time_to_checkpoint():
        self.checkpoint(checkpointer)
```

### Transform Phase (`transform_realtime_phase.py`)

**Purpose**: Data enrichment, validation, and business logic application

**Key Responsibilities**:

- Parse raw telemetry into structured `RealtimeRecord`
- Validate asset types (filter haul trucks only)
- Prevent duplicate processing using temporal validation
- Perform spatial analysis (loader proximity, dump region detection)
- Apply cycle state management logic
- Generate cycle records and process checkpoints

**Function**: `process_new_record()`

**Processing Stages**:

```python
def process_new_record(pgsql_conn, redis_conn, current_record_data):
    # STAGE 1: Parse Raw Record
    curr_rec = RealtimeRecord(**current_record_data)

    # STAGE 2: Asset Validation
    if not is_the_asset_cycle_truck(pgsql_conn, curr_rec.asset_guid):
        return (None, None, None, None)

    # STAGE 3: Temporal Validation
    if is_current_record_older(pgsql_conn, curr_rec):
        return (None, None, None, None)

    # STAGE 4: Get Last Cycle State
    last_cycle_rec = get_last_cycle_details(pgsql_conn, curr_rec)

    # STAGE 5A: Spatial Analysis - Loader Proximity
    nearest_loader, loader_distance = is_truck_near_any_loader(...)

    # STAGE 5B: Spatial Analysis - Dump Region Detection
    within_dump_region = is_truck_within_dump_region(...)

    # STAGE 6: Cycle State Decision Logic
    if is_init_record:
        init_record = record_initialization(curr_rec)
    else:
        # Determine truck position context
        position = determine_position(nearest_loader, within_dump_region)

        # Apply business logic
        context = CycleComparisonContext(...)
        updated_record, new_cycle = cycle_records_comparison(context)

    # STAGE 7: Generate Process Info
    pi_record = get_latest_process_info(...)

    return (init_record, updated_record, new_cycle, pi_record)
```

**Data Enrichment Sources**:

- **PostgreSQL**: Asset metadata, loader positions, cycle history
- **Redis**: Dump region geometries, spatial lookup data

**Business Logic Integration**:

- Routes to `cores/segment_decision.py` for state management
- Applies mining-specific business rules and thresholds
- Calculates spatial relationships and timing metrics

### Load Phase (`load_realtime_phase.py`)

**Purpose**: Database persistence and checkpoint management

**Key Responsibilities**:

- Write initial cycle records for new truck operations
- Update existing cycle records with state changes
- Archive completed cycles to final table
- Manage processing checkpoints for duplicate prevention
- Update in-memory cache for performance optimization

**Function**: `load_process()`

**Database Operations**:

```python
def load_process(pgsql_conn, initial_cycle_record, last_cycle_record_updated,
                new_cycle_record, process_info_record):

    # 1. Initial Record Creation
    if initial_cycle_record:
        pgsql_conn.insert_new_data(initial_cycle_record)

    # 2. Cycle Updates and Archival
    if last_cycle_record_updated:
        pgsql_conn.update_data(last_cycle_record_updated)

        # Archive completed cycles
        if last_cycle_record_updated.cycle_status in CYCLES_FOR_FINAL_TABLE:
            pgsql_conn.insert_new_final_cycle_data(last_cycle_record_updated)

    # 3. New Cycle Creation
    if new_cycle_record:
        pgsql_conn.insert_new_data(new_cycle_record)

    # 4. Checkpoint Management
    if process_info_record:
        if process_info_record.is_new_record():
            pgsql_conn.insert_new_data(process_info_record)
        else:
            pgsql_conn.update_data(process_info_record)

        # Update in-memory cache
        checkpoint.LATEST_PROCESS_INFO[asset_guid] = process_date
```

**Performance Optimizations**:

- **Batch Operations**: Uses `execute_values()` for efficient multi-record writes
- **Transaction Management**: Automatic commit/rollback with error handling
- **In-Memory Caching**: Reduces database load for checkpoint validation

**Table Management**:

- **Temporary Table**: Active cycles (`INPROGRESS` status)
- **Final Table**: Completed cycles (`COMPLETE`, `INVALID`, `OUTLIER` status)
- **Checkpoint Table**: Processing state for duplicate prevention

### Preprocessing Utilities (`preprocess.py`)

**Purpose**: Shared utility functions for data processing and validation

**Key Functions**:

#### Asset Validation
```python
def is_the_asset_cycle_truck(pgsql_conn, asset_guid, site_guid) -> bool:
    """Validate asset is a trackable haul truck"""
    # Queries dx.asset table for asset_type_guid validation
```

#### Temporal Validation
```python
def is_current_record_older(pgsql_conn, current_record) -> bool:
    """Prevent processing of duplicate/old data"""
    # Checks in-memory cache first, then database
```

#### Spatial Analysis
```python
def is_truck_near_any_loader(truck_location, list_of_loaders) -> Tuple[LoaderAsset, float]:
    """Calculate proximity to loader assets using Haversine distance"""

def is_truck_within_dump_region(truck_location, list_of_dump_regions) -> RegionPoly:
    """Point-in-polygon testing for dump region geofencing"""
```

#### Data Retrieval
```python
def get_last_cycle_details(pgsql_conn, site_guid, asset_guid) -> CycleRecord:
    """Retrieve most recent in-progress cycle for asset"""

def get_all_dump_regions(redis_conn, site_guid) -> List[RegionPoly]:
    """Load dump region boundaries from Redis cache"""
```

#### Record Initialization
```python
def record_initialization(current_record) -> CycleRecord:
    """Create new cycle record with default values"""
```

**Integration Points**:

- Called by Transform Phase for data enrichment
- Interfaces with DAO layer for database operations
- Provides spatial analysis capabilities

## Business Logic Engine (`cores/`)

### Identifiers (`cores/identifiers.py`)

**Purpose**: Constants, enums, and factory classes for business logic

**Key Components**:

#### Enums
```python
class WorkState(IntEnum):
    IDLING = 2      # Truck stationary
    WORKING = 3     # Truck moving

class CycleStatus(str, Enum):
    INPROGRESS = "INPROGRESS"  # Active cycle
    COMPLETE = "COMPLETE"      # All segments completed
    INVALID = "INVALID"        # Missing/abnormal segments
    OUTLIER = "OUTLIER"        # Extended idle time

class CycleSegment(str, Enum):
    LOAD_TIME = "LOAD_TIME"        # At loader
    LOAD_TRAVEL = "LOAD_TRAVEL"    # Loaded travel
    DUMP_TIME = "DUMP_TIME"        # At dump
    EMPTY_TRAVEL = "EMPTY_TRAVEL"  # Empty return
```

#### Factory Pattern
```python
class CycleRecordFactory:
    @staticmethod
    def create_base_params(last_rec, curr_rec) -> Dict:
        """Create parameters for updating existing records"""

    @staticmethod
    def create_new_cycle_params(curr_rec, last_rec, loader_rec, context) -> Dict:
        """Create parameters for new cycle initialization"""

    @staticmethod
    def create_outlier_recovery_params(curr_rec, last_rec) -> Dict:
        """Create minimal cycle after outlier closure"""
```

#### Handler Interface
```python
class CycleStateHandler(ABC):
    @abstractmethod
    def handle(self, context: CycleComparisonContext) -> Tuple[CycleRecord, CycleRecord]:
        """Handle state transition and return updated/new records"""
```

### Segment Decision Engine (`cores/segment_decision.py`)

**Purpose**: Core business logic for cycle state management

**Main Entry Point**:
```python
def cycle_records_comparison(context: CycleComparisonContext) -> Tuple[CycleRecord, CycleRecord]:
    """Process cycle comparison and return updated/new records"""
```

**Handler Architecture**:

- **LoadingAreaHandler**: Manages loading operations and cycle initialization
- **DumpingAreaHandler**: Manages dumping operations
- **TravelingAreaHandler**: Detects outlier behavior during travel
- **CycleSegmentDecisionEngine**: Routes to appropriate handler

**Business Rules Implementation**:

- **Idle Thresholds**: Configurable time limits for state transitions
- **Distance Calculations**: Proximity-based loading area detection
- **Outlier Detection**: Extended idle time marking and recovery
- **Cycle Closure**: State-based cycle completion logic

**State Machine**:
```
Loading Area:    None → LOAD_TIME → LOAD_TRAVEL
Dumping Area:    LOAD_TRAVEL → DUMP_TIME → EMPTY_TRAVEL
Traveling Area:  Any segment + extended idle → OUTLIER
```

## Data Flow Integration

### End-to-End Processing
```
1. Kinesis Record → Extract Phase (RecordProcessor)
2. JSON Parsing → Transform Phase (process_new_record)
3. Asset Validation → Preprocessing (is_the_asset_cycle_truck)
4. Spatial Analysis → Preprocessing (proximity/geofencing)
5. Business Logic → Cores (cycle_records_comparison)
6. Database Writes → Load Phase (load_process)
7. Checkpoint Update → In-memory cache
```

### Error Handling Strategy
- **Extract Phase**: Log errors, continue processing other records
- **Transform Phase**: Return None values, skip database writes
- **Load Phase**: Database rollback, retry logic in DAO layer
- **Business Logic**: Graceful degradation, warning logs

### Performance Characteristics
- **Throughput**: ~100-500 records/second per ECS task
- **Latency**: Sub-second processing from Kinesis to database
- **Memory Usage**: ~512MB-1GB per consumer (Python + Java KCL)
- **Database Load**: Optimized with connection pooling and batch operations

## Configuration

### Environment Variables
- `TX_TYPE`: Processing mode (`realtime` or `reprocess`)
- Database connection secrets via AWS Secrets Manager
- Kinesis stream configuration via KCL properties

### Business Logic Thresholds (`config/static_config.py`)
```python
IDLE_THRESHOLD_FOR_START_LOAD = 20.0      # Seconds before loading starts
IDLE_THRESHOLD_FOR_START_DUMP = 20.0      # Seconds before dumping starts
IDLE_THRESHOLD_IN_TRAVEL_SEGMENT = 600.0  # Seconds before marking outlier
DISTANCE_THRESHOLD_FOR_LOADER = 50.0      # Meters for loader proximity
```

## Monitoring and Debugging

### Key Metrics
- **Processing Rate**: Records processed per second
- **Error Rate**: Failed processing attempts
- **Cycle Completion**: Distribution of cycle statuses
- **Memory Usage**: Python and Java process monitoring

### Logging Strategy
- **Structured JSON**: Consistent log format for parsing
- **Performance Timing**: Duration tracking for each phase
- **Business Events**: Cycle state transitions and decisions
- **Error Context**: Full stack traces with business context

### Common Debugging Scenarios
1. **Processing Lag**: Check Kinesis shard iterator age
2. **Memory Issues**: Monitor Java KCL daemon memory usage
3. **Data Quality**: Review outlier detection and invalid cycles
4. **Database Errors**: Connection pool exhaustion or timeout issues

## Testing Strategy

### Unit Testing
- **Transform Phase**: Mock database connections, test business logic
- **Business Logic**: Test state machine transitions and edge cases
- **Spatial Analysis**: Validate proximity and geofencing calculations

### Integration Testing
- **End-to-End**: Kinesis → Database with test data
- **Error Scenarios**: Network failures, invalid data formats
- **Performance**: Load testing with realistic data volumes

## Contributing

### Code Organization
- **Single Responsibility**: Each phase has clear, focused purpose
- **Dependency Injection**: Database connections passed as parameters
- **Error Handling**: Consistent exception handling and logging
- **Type Safety**: Comprehensive type hints throughout

### Adding New Features
1. **Business Logic**: Extend handlers in `cores/segment_decision.py`
2. **Data Enrichment**: Add functions to `preprocess.py`
3. **Database Operations**: Extend DAO layer methods
4. **Configuration**: Add new thresholds to `static_config.py`

### Performance Considerations
- **Database Queries**: Minimize roundtrips, use batch operations
- **Memory Management**: Avoid large object creation in hot paths
- **Caching Strategy**: Balance between memory usage and query performance
- **Spatial Operations**: Optimize polygon parsing and point-in-polygon testing