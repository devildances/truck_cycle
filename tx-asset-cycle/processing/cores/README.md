# Haul Truck Cycle Management System

## Overview

The Cycle Segment Decision module tracks and manages the operational cycles of haul trucks in mining operations. It monitors trucks as they move between loading areas, dumping areas, and travel segments, calculating cycle times and validating cycle completeness.

## Business Context

In mining operations, haul trucks follow a repetitive cycle:

1. **Loading** - Truck is loaded with material at a loader
2. **Loaded Travel** - Truck travels to dump area with material
3. **Dumping** - Truck dumps material at designated dump region
4. **Empty Travel** - Truck returns empty to loader for next cycle

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

## Cycle Status Types

| Status | Description |
|--------|-------------|
| **INPROGRESS** | Cycle is currently being executed |
| **COMPLETE** | All four segments executed with valid durations |
| **INVALID** | Missing segments or abnormal transitions |
| **OUTLIER** | Cycle terminated due to extended idle in travel segments |

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

**Scenarios**:

1. **Initial Load**: First cycle after system startup
2. **Normal Return**: Truck returns after completing full cycle
3. **Abnormal Return**: Truck returns directly from LOAD_TRAVEL (dumped outside region)

### Dumping Area

**Purpose**: Manages dumping operations

**Key Business Rules**:

- Truck must idle for minimum threshold (IDLE_THRESHOLD_FOR_START_DUMP) before dumping starts
- Dumping completes when truck starts moving
- Only updates existing cycles, never creates new ones
- Transitions to EMPTY_TRAVEL after dumping

**Scenarios**:

1. **Normal Arrival**: From LOAD_TRAVEL segment
2. **Unexpected Arrival**: From other segments (logged as warning)

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

## Time Tracking

### Segment Durations
- **load_seconds**: Time spent loading at loader
- **load_travel_seconds**: Time traveling from loader to dump
- **dump_seconds**: Time spent dumping material
- **empty_travel_seconds**: Time returning from dump to loader
- **outlier_seconds**: Accumulated idle time in travel segments
- **total_cycle_seconds**: Sum of all segment durations (exclude outlier_seconds)

### Timestamps
- **load_start_utc**: When loading began
- **load_end_utc**: When truck left loader
- **dump_start_utc**: When dumping began
- **dump_end_utc**: When truck left dump area

## Distance Tracking

- **previous_loader_distance**: Distance to loader at previous update
- **current_loader_distance**: Distance to loader at current update

Used to track truck proximity to loading equipment.

## Data Flow

### Input (CycleComparisonContext)
- **current_record**: Current GPS/state data from truck
- **last_record**: Previous cycle record from database
- **asset_position**: Current area (loading/dumping/traveling)
- **loader_asset**: Nearby loader information (if applicable)
- **loader_distance**: Distance to nearest loader
- **dump_region**: Dump region information (if applicable)

### Output
- **Updated Record**: Modified version of last record (if changes needed)
- **New Record**: New cycle record (only in specific scenarios)

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
| IDLE_THRESHOLD_FOR_START_LOAD | Minimum idle time before loading starts | 300 seconds (5 min) |
| IDLE_THRESHOLD_FOR_START_DUMP | Minimum idle time before dumping starts | 180 seconds (3 min) |
| IDLE_THRESHOLD_IN_TRAVEL_SEGMENT | Maximum idle time before marking as outlier | 600 seconds (10 min) |
