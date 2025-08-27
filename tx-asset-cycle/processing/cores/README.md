# Haul Truck Cycle Management System

## Overview

The Cycle Segment Decision module tracks and manages the operational cycles of haul trucks in mining operations. It monitors trucks as they move between loading areas, dumping areas, and travel segments, calculating cycle times and validating cycle completeness. The module also handles special scenarios such as when trucks and loaders are co-located in the same dump area.

## Business Context

In mining operations, haul trucks follow a repetitive cycle:

1. **Loading** - Truck is loaded with material at a loader
2. **Loaded Travel** - Truck travels to dump area with material
3. **Dumping** - Truck dumps material at designated dump region
4. **Empty Travel** - Truck returns empty to loader for next cycle

Additionally, the system monitors special operational patterns where trucks and loaders may be in the same dump area, which indicates unusual operational scenarios requiring specific handling.

## Cycle Flow Patterns

### Normal Cycle Flow
```
LOAD_TIME → LOAD_TRAVEL → DUMP_TIME → EMPTY_TRAVEL → LOAD_TIME (new cycle)
```

### Abnormal Cycle Flow
```
LOAD_TIME → LOAD_TRAVEL → LOAD_TIME (new cycle)
```
This occurs when a truck dumps material outside designated dump regions, skipping DUMP_TIME and EMPTY_TRAVEL segments.

### Special Operational Patterns
- **Truck and Loader Co-location**: When both assets are detected in the same dump area
- **Asset Separation**: When previously co-located assets separate, triggering cycle closure

## Cycle Status Types

| Status | Description |
|--------|-------------|
| **INPROGRESS** | Cycle is currently being executed |
| **COMPLETE** | All four segments executed with valid durations |
| **INVALID** | Missing segments or abnormal transitions |
| **OUTLIER** | Cycle terminated due to extended idle in travel segments or unusual operational patterns |

## Work States

| State | Value | Description |
|-------|-------|-------------|
| **IDLING** | 2 | Truck is stationary |
| **WORKING** | 3 | Truck is in motion |

## Area-Specific Business Rules

### Loading Area

**Purpose**: Manages loading operations and cycle initialization

**Key Business Rules**:

- Truck must idle for minimum threshold (IDLE_THRESHOLD_FOR_START_LOAD) before loading starts
- Loading completes when truck starts moving (IDLING → WORKING transition)
- Handles cycle closure when truck returns from other areas
- Creates new cycles after closing previous ones
- Detects and handles outlier behavior for extended idle periods

**Scenarios**:

1. **Initial Load**: First cycle after system startup
2. **Normal Return**: Truck returns after completing full cycle
3. **Abnormal Return**: Truck returns directly from LOAD_TRAVEL (dumped outside region)
4. **Implied Loading**: Loading occurred between GPS updates
5. **Outlier Recovery**: Recovery from extended idle periods

### Dumping Area

**Purpose**: Manages dumping operations

**Key Business Rules**:

- Truck must idle for minimum threshold (IDLE_THRESHOLD_FOR_START_DUMP) before dumping starts
- Dumping completes when truck starts moving
- Transitions to EMPTY_TRAVEL after dumping
- Handles outlier behavior for extended idle periods
- Creates new cycles when recovering from outlier status

**Scenarios**:

1. **Normal Arrival**: From LOAD_TRAVEL segment
2. **Unexpected Arrival**: From other segments (logged as warning)
3. **Implied Dumping**: Dumping occurred between GPS updates
4. **Outlier Recovery**: Recovery from extended idle at dump

### Traveling Area

**Purpose**: Detects and handles outlier behavior during travel

**Key Business Rules**:

- Monitors for extended idle time during travel segments
- Marks cycles as outliers if idle exceeds IDLE_THRESHOLD_IN_TRAVEL_SEGMENT
- Creates new cycles when outlier cycles are closed
- Tracks outlier position (GPS coordinates)
- Accumulates outlier duration if truck idles multiple times

**Outlier Handling**:

1. **Detection**: Truck idles too long → marked as outlier
2. **Closure**: When outlier truck starts moving → close cycle as OUTLIER
3. **Recovery**: Create minimal new cycle for continued tracking

### Same Dump Area (Special Handler)

**Purpose**: Manages cycles when truck and loader are in the same dump area

**Key Business Rules**:

- Monitors the `all_assets_in_same_dump_area` flag
- Updates flag when assets enter same dump area together
- Closes cycle as OUTLIER when assets separate
- Creates new cycle for continued tracking after separation
- Takes precedence over position-based handlers when active

**State Transitions**:

1. **Not Together → Together**: Set flag to True
2. **Together → Still Together**: No action needed
3. **Together → Separated**: Close cycle as OUTLIER, create new cycle

## Time Tracking

### Segment Durations
- **load_seconds**: Time spent loading at loader
- **load_travel_seconds**: Time traveling from loader to dump
- **dump_seconds**: Time spent dumping material
- **empty_travel_seconds**: Time returning from dump to loader
- **outlier_seconds**: Accumulated idle time in travel segments or extended idle at geofence areas
- **total_cycle_seconds**: Sum of all segment durations (excludes outlier_seconds)

### Timestamps
- **load_start_utc**: When loading began
- **load_end_utc**: When truck left loader
- **dump_start_utc**: When dumping began
- **dump_end_utc**: When truck left dump area
- **cycle_start_utc**: When cycle began
- **cycle_end_utc**: When cycle ended

## Special Tracking Fields

### Co-location Tracking
- **all_assets_in_same_dump_area**: Boolean flag indicating if truck and loader are in same dump area
- **assets_in_same_location**: Context field indicating current co-location status

### Distance Tracking
- **previous_loader_distance**: Distance to loader at previous update
- **current_loader_distance**: Distance to loader at current update

### Outlier Tracking
- **is_outlier**: Boolean flag for outlier status
- **outlier_position_latitude**: GPS latitude where outlier behavior detected
- **outlier_position_longitude**: GPS longitude where outlier behavior detected
- **outlier_seconds**: Total accumulated outlier idle time

## Data Flow

### Input (CycleComparisonContext)
- **current_record**: Current GPS/state data from truck
- **last_record**: Previous cycle record from database
- **asset_position**: Current area (loading/dumping/traveling)
- **loader_asset**: Nearby loader information (if applicable)
- **loader_distance**: Distance to nearest loader
- **dump_region**: Dump region information (if applicable)
- **assets_in_same_location**: Co-location status (optional)

### Output
- **Updated Record**: Modified version of last record (if changes needed)
- **New Record**: New cycle record (only in specific scenarios)

## Handler Priority

The CycleSegmentDecisionEngine processes handlers in the following priority:

1. **Same Dump Area Handler**: Checked first if `assets_in_same_location` is not None OR `all_assets_in_same_dump_area` is True
2. **Position-based Handlers**: Used if same dump area handler doesn't apply
   - Loading Area Handler
   - Dumping Area Handler
   - Traveling Area Handler

## Integration Points

### AWS Kinesis (Input)
- Real-time truck position and state data
- Streaming updates trigger cycle processing

### PostgreSQL (Storage)
- Stores cycle records
- Provides historical data for comparison

## Configuration Parameters

| Parameter | Description | Typical Value |
|-----------|-------------|---------------|
| IDLE_THRESHOLD_FOR_START_LOAD | Minimum idle time before loading starts | 20 seconds |
| IDLE_THRESHOLD_FOR_START_DUMP | Minimum idle time before dumping starts | 15 seconds |
| IDLE_THRESHOLD_IN_TRAVEL_SEGMENT | Maximum idle time before marking as outlier | 600 seconds (10 min) |
| IDLE_THRESHOLD_IN_GEOFENCE_AREA | Maximum idle time in geofence areas | 1200 seconds (20 min) |
| DISTANCE_MAX_FOR_LOADER_MOVEMENT | Maximum distance for same loader validation | 10 meters |

## Key Features

### Implied Operations Handling
The module handles scenarios where loading or dumping operations occur between GPS updates:
- **Implied Loading**: Detects when loading occurred during idle period at loader
- **Implied Dumping**: Detects when dumping occurred during idle period at dump

### Outlier Detection and Recovery
- Detects extended idle periods in any area
- Marks cycles as outliers when thresholds exceeded
- Creates new cycles for continued tracking after outlier recovery

### Co-location Management
- Tracks when truck and loader are in same dump area
- Handles cycle closure when assets separate
- Provides insights into unusual operational patterns

### Robust State Management
- Handles all state transitions gracefully
- Maintains cycle continuity despite GPS gaps
- Supports both normal and abnormal operational flows