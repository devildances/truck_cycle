# Code Logic Flow Documentation

## Entry Point Flow

```
cycle_records_comparison(context)
    ↓
CycleSegmentDecisionEngine()
    ↓
engine.process_context(context)
    ↓
handler = handlers[context.asset_position]
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
    OUTLIER        # Extended idle

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
    │   │       ├─ If load_start_utc is None:
    │   │       │   ├─ Calculate idle_duration
    │   │       │   └─ If > IDLE_THRESHOLD_FOR_START_LOAD:
    │   │       │       └─ _start_new_cycle()
    │   │       │
    │   │       └─ Else: Return (None, None)
    │   │
    │   └─ Different States (Transition):
    │       └─ _handle_state_transition():
    │           │
    │           ├─ WORKING → IDLING:
    │           │   └─ Update work state only
    │           │
    │           └─ IDLING → WORKING:
    │               │
    │               ├─ If segment is None:
    │               │   └─ Update work state only
    │               │
    │               ├─ If in LOAD_TIME with load_start_utc:
    │               │   ├─ Calculate load_seconds
    │               │   ├─ Set segment = LOAD_TRAVEL
    │               │   └─ Set load_end_utc
    │               │
    │               └─ Else: Update work state only
    │
    └─ _start_new_cycle():
        │
        ├─ If current_segment is None (Initial):
        │   ├─ Validate loader_asset exists
        │   ├─ Set segment = LOAD_TIME
        │   ├─ Set loader info
        │   └─ Return (updated_record, None)
        │
        ├─ If already in LOAD_TIME:
        │   └─ Return (None, None)
        │
        └─ Else (Close previous + New):
            ├─ _close_previous_cycle():
            │   │
            │   ├─ If LOAD_TRAVEL → LOAD_TIME:
            │   │   ├─ Status = INVALID
            │   │   └─ Calculate load_travel_seconds
            │   │
            │   └─ If EMPTY_TRAVEL → LOAD_TIME:
            │       ├─ Calculate empty_travel_seconds
            │       ├─ Check all segments present
            │       ├─ If complete: Status = COMPLETE
            │       └─ Else: Status = INVALID
            │
            └─ Create new cycle record
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
    │               ├─ If already in DUMP_TIME:
    │               │   └─ Return (None, None)
    │               │
    │               ├─ If in LOAD_TRAVEL:
    │               │   ├─ Validate dump_region exists
    │               │   ├─ Calculate load_travel_seconds
    │               │   ├─ Set segment = DUMP_TIME
    │               │   └─ Set dump_start_utc
    │               │
    │               └─ Other segments:
    │                   └─ Transition to DUMP_TIME (warning)
    │
    └─ State Transition:
        │
        ├─ WORKING → IDLING:
        │   └─ Update work state only
        │
        └─ IDLING → WORKING:
            │
            ├─ If in DUMP_TIME:
            │   ├─ Calculate dump_seconds
            │   ├─ Set segment = EMPTY_TRAVEL
            │   └─ Set dump_end_utc
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
            └─ Else: Update work state only
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
```

### Error Patterns
```
Missing Required Data → Log ERROR → Return (None, None)
Invalid State → Log WARNING → Continue with fallback
Unexpected Segment → Log WARNING → Handle gracefully
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
   ↓
5. Call cycle_records_comparison()
   ↓
6. Process through appropriate handler
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
```

### Dumping Area States
```
LOAD_TRAVEL → DUMP_TIME (arrival)
DUMP_TIME → EMPTY_TRAVEL (leaving)
```

### Traveling Area States
```
Any segment + extended idle → is_outlier = True
is_outlier + start moving → Close as OUTLIER + New cycle
```

## Key Decision Points

1. **Work State Check**: Determines if transition or continuous state
2. **Idle Duration**: Triggers segment changes based on thresholds
3. **Current Segment**: Determines valid transitions and validations
4. **is_outlier Flag**: Special handling for problematic cycles
5. **Timestamp Presence**: Validates if operations started/completed

## Return Value Patterns

| Scenario | Updated Record | New Record |
|----------|----------------|------------|
| No change needed | None | None |
| State update only | Updated | None |
| Segment transition | Updated | None |
| Cycle closure + new | Updated (closed) | New cycle |
| Initial record setup | Updated | None |
| Outlier closure | Updated (OUTLIER) | New minimal |
