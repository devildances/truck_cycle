# Code Logic Flow Documentation

## Entry Point Flow

```
cycle_records_comparison(context)
    ↓
CycleSegmentDecisionEngine()
    ↓
engine.process_context(context)
    ↓
Check if assets_in_same_location is not None
OR all_assets_in_same_dump_area is True
    ├─ Yes: handler = handlers["same_dump_area"]
    └─ No: handler = handlers[context.asset_position]
    ↓
handler.handle(context)
    ↓
Returns: (updated_record, new_record)
```

## Core Components

### 1. Enums and Constants

```python
WorkState(IntEnum):
    IDLING = 2      # Truck stationary
    WORKING = 3     # Truck moving

CycleStatus(Enum):
    INPROGRESS     # Active cycle
    COMPLETE       # All segments done
    INVALID        # Missing segments
    OUTLIER        # Extended idle or unusual pattern

CycleSegment(Enum):
    LOAD_TIME      # At loader
    LOAD_TRAVEL    # Loaded travel
    DUMP_TIME      # At dump
    EMPTY_TRAVEL   # Empty return
```

### 2. Factory Pattern

```python
CycleRecordFactory:
    create_base_params()           # Full record copy
    create_new_cycle_params()      # New cycle with loader
    create_outlier_recovery_params() # Minimal cycle after outlier
```

## Handler-Specific Logic Flows

### LoadingAreaHandler Flow

```
handle(context):
    │
    ├─ Check Work States:
    │   │
    │   ├─ WORKING + WORKING (Moving):
    │   │   └─ Return (None, None) - No action
    │   │
    │   ├─ IDLING + IDLING (Idling):
    │   │   └─ _handle_idling_near_loader():
    │   │       │
    │   │       ├─ Calculate idle_duration
    │   │       └─ If > IDLE_THRESHOLD_FOR_START_LOAD:
    │   │           │
    │   │           ├─ If in LOAD_TIME:
    │   │           │   └─ _load_time_idling_handler():
    │   │           │       ├─ If is_outlier: Return (None, None)
    │   │           │       └─ If > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
    │   │           │           └─ Mark as outlier
    │   │           │
    │   │           └─ Else: _start_new_cycle()
    │   │
    │   └─ Different States (Transition):
    │       └─ _handle_state_transition():
    │           │
    │           ├─ WORKING → IDLING:
    │           │   └─ Update work state only
    │           │
    │           └─ IDLING → WORKING:
    │               │
    │               ├─ If is_outlier:
    │               │   └─ _handle_outlier_load_completion()
    │               │
    │               ├─ If idle > IDLE_THRESHOLD_FOR_START_LOAD:
    │               │   └─ _handle_implied_load_completion()
    │               │
    │               ├─ If in LOAD_TIME with load_start_utc:
    │               │   └─ Complete loading → LOAD_TRAVEL
    │               │
    │               └─ Else: Update work state only
```

### DumpingAreaHandler Flow

```
handle(context):
    │
    ├─ IDLING + IDLING (Continuous Idle):
    │   └─ _handle_continuous_idling():
    │       ├─ Calculate idle_duration
    │       └─ If > IDLE_THRESHOLD_FOR_START_DUMP:
    │           └─ _handle_idle_threshold_exceeded():
    │               │
    │               ├─ If is_outlier: Return (None, None)
    │               │
    │               ├─ If > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
    │               │   └─ Mark as outlier
    │               │
    │               ├─ If in DUMP_TIME:
    │               │   └─ Return (None, None)
    │               │
    │               ├─ If in LOAD_TRAVEL:
    │               │   └─ Transition to DUMP_TIME
    │               │
    │               └─ Other segments:
    │                   └─ Transition to DUMP_TIME (warning)
    │
    └─ State Transition:
        │
        ├─ WORKING → IDLING:
        │   └─ Update work state + set idle_in_dump_region_guid
        │
        └─ IDLING → WORKING:
            │
            ├─ If is_outlier or idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
            │   └─ _handle_outlier_dump_completion()
            │
            ├─ If idle > IDLE_THRESHOLD_FOR_START_DUMP:
            │   └─ _handle_implied_dump_completion()
            │
            ├─ If in DUMP_TIME:
            │   └─ Complete dumping → EMPTY_TRAVEL
            │
            └─ Else: Update work state only
```

### TravelingAreaHandler Flow

```
handle(context):
    │
    ├─ IDLING + IDLING (Continuous Idle):
    │   └─ _handle_continuous_idling():
    │       ├─ Calculate idle_duration
    │       └─ If > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT:
    │           └─ _mark_as_outlier():
    │               │
    │               ├─ If already is_outlier:
    │               │   └─ Accumulate outlier_seconds
    │               │
    │               └─ Else:
    │                   ├─ Set is_outlier = True
    │                   ├─ Set outlier_position_latitude/longitude
    │                   └─ Set outlier_seconds
    │
    └─ State Transition:
        │
        ├─ WORKING → IDLING:
        │   └─ Update work state only
        │
        └─ IDLING → WORKING:
            │
            ├─ If is_outlier:
            │   └─ _close_outlier_cycle():
            │       ├─ Set cycle_status = OUTLIER
            │       ├─ Create minimal new cycle
            │       └─ Return (updated, new)
            │
            ├─ If idle > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT:
            │   └─ _mark_as_outlier()
            │
            └─ Else: Update work state only
```

### AllAssetsInSameDumpAreaHandler Flow

```
handle(context):
    │
    ├─ Check Co-location Status:
    │   │
    │   ├─ Not Together → Together (False → True):
    │   │   └─ _handle_assets_newly_together():
    │   │       ├─ Create base_params
    │   │       ├─ Set all_assets_in_same_dump_area = True
    │   │       └─ Return (updated_record, None)
    │   │
    │   ├─ Together → Still Together (True → True):
    │   │   └─ Return (None, None) - No action
    │   │
    │   ├─ Together → Separated (True → False):
    │   │   └─ _handle_assets_separated():
    │   │       ├─ Close cycle with status = OUTLIER
    │   │       ├─ Set cycle_end_utc
    │   │       ├─ Create new cycle (outlier recovery)
    │   │       └─ Return (updated, new)
    │   │
    │   └─ Separated → Still Separated (False → False):
    │       └─ Return (None, None) - No action
```

## Data Structure Updates

### Record Creation Patterns

1. **Full Copy Update** (create_base_params):
   ```python
   - Copies all fields from last_record
   - Updates work states
   - Sets new updated_date
   - Preserves historical data
   ```

2. **New Cycle Creation** (create_new_cycle_params):
   ```python
   - Increments cycle_number
   - Sets status = INPROGRESS
   - Sets segment = LOAD_TIME
   - Initializes loader information
   - All durations = None
   ```

3. **Outlier Recovery** (create_outlier_recovery_params):
   ```python
   - Increments cycle_number
   - Sets status = INPROGRESS
   - segment = None (unknown position)
   - No loader/dump information
   - Minimal initialization
   ```

## Handler Priority and Selection

### CycleSegmentDecisionEngine.process_context()

```
1. Check Same Dump Area Conditions:
   - If assets_in_same_location is not None
   - OR all_assets_in_same_dump_area is True
   → Use AllAssetsInSameDumpAreaHandler

2. Otherwise Position-based Selection:
   - "loading_area" → LoadingAreaHandler
   - "dumping_area" → DumpingAreaHandler
   - "traveling_area" → TravelingAreaHandler
```

## Validation and Error Handling

### Context Validation
```
1. Check current_record exists
2. Check last_record exists
3. Verify asset_position is valid
4. Area-specific validation:
   - Loading: Need loader_asset for initial records
   - Dumping: Need dump_region for transitions
   - Traveling: GPS coordinates for outlier position
   - Same Dump: Check assets_in_same_location flag
```

### Error Patterns
```
Missing Required Data → Log ERROR → Return (None, None)
Invalid State → Log WARNING → Continue with fallback
Unexpected Segment → Log WARNING → Handle gracefully
No Handler Found → Log WARNING → Return (None, None)
```

## Integration Flow

### Kinesis → Module → PostgreSQL

```
1. Kinesis Record Received
   ↓
2. Parse to RealtimeRecord
   ↓
3. Fetch last CycleRecord from DB
   ↓
4. Create CycleComparisonContext
   - Include assets_in_same_location if applicable
   ↓
5. Call cycle_records_comparison()
   ↓
6. Process through appropriate handler
   - Check same dump area first if applicable
   - Otherwise use position-based routing
   ↓
7. Return (updated, new) records
   ↓
8. Write to PostgreSQL:
   - UPDATE if updated_record
   - INSERT if new_record
```

## State Machine Summary

### Loading Area States
```
None → LOAD_TIME (initial)
LOAD_TIME → LOAD_TRAVEL (loaded, leaving)
EMPTY_TRAVEL → LOAD_TIME (normal return)
LOAD_TRAVEL → LOAD_TIME (abnormal return)
Any + outlier → Close as OUTLIER + New cycle
```

### Dumping Area States
```
LOAD_TRAVEL → DUMP_TIME (arrival)
DUMP_TIME → EMPTY_TRAVEL (leaving)
Any + outlier → Close as OUTLIER + New cycle
```

### Traveling Area States
```
Any segment + extended idle → is_outlier = True
is_outlier + start moving → Close as OUTLIER + New cycle
```

### Same Dump Area States
```
all_assets_in_same_dump_area = False → True (assets together)
all_assets_in_same_dump_area = True → True (no change)
all_assets_in_same_dump_area = True → False (close as OUTLIER + New)
```

## Key Decision Points

1. **Handler Selection**: Same dump area check takes precedence
2. **Work State Check**: Determines if transition or continuous state
3. **Idle Duration**: Triggers segment changes based on thresholds
4. **Current Segment**: Determines valid transitions and validations
5. **is_outlier Flag**: Special handling for problematic cycles
6. **all_assets_in_same_dump_area Flag**: Triggers special handler
7. **Timestamp Presence**: Validates if operations started/completed

## Return Value Patterns

| Scenario | Updated Record | New Record |
|----------|----------------|------------|
| No change needed | None | None |
| State update only | Updated | None |
| Segment transition | Updated | None |
| Cycle closure + new | Updated (closed) | New cycle |
| Initial record setup | Updated | None |
| Outlier closure | Updated (OUTLIER) | New minimal |
| Assets newly together | Updated (flag=True) | None |
| Assets separated | Updated (OUTLIER) | New minimal |

## Threshold Summary

| Threshold | Value | Purpose |
|-----------|-------|---------|
| IDLE_THRESHOLD_FOR_START_LOAD | 20s | Start loading detection |
| IDLE_THRESHOLD_FOR_START_DUMP | 15s | Start dumping detection |
| IDLE_THRESHOLD_IN_TRAVEL_SEGMENT | 600s | Outlier in travel areas |
| IDLE_THRESHOLD_IN_GEOFENCE_AREA | 1200s | Outlier in geofence areas |
| DISTANCE_MAX_FOR_LOADER_MOVEMENT | 10m | Same loader validation |

## Special Features

### Implied Operations
- **Implied Loading**: Detected when truck was idle > threshold at loader and is now moving
- **Implied Dumping**: Detected when truck was idle > threshold at dump and is now moving

### Outlier Recovery
- All handlers can now detect and recover from outlier conditions
- Creates new cycles to maintain tracking continuity
- Preserves outlier information for analysis

### Co-location Management
- Special handler for truck-loader co-location in dump areas
- Overrides position-based routing when active
- Handles unusual operational patterns