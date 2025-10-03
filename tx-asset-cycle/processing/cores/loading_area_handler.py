import json
import logging
from datetime import timedelta
from typing import Optional, Self, Tuple

from config.static_config import (DISTANCE_MAX_FOR_LOADER_MOVEMENT,
                                  IDLE_THRESHOLD_FOR_START_LOAD,
                                  IDLE_THRESHOLD_IN_GEOFENCE_AREA,
                                  MAX_IDLE_THRESHOLD_TO_CLOSE_STATE)
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import (DurationCalculator, haversine_distance,
                             timestamp_to_utc_zero)

from .identifiers import (CycleRecordFactory, CycleSegment, CycleStateHandler,
                          CycleStatus, WorkState)

logger = logging.getLogger(__name__)


class LoadingAreaHandler(CycleStateHandler):
    """Handler for loading area cycle logic with comprehensive state management.

    Manages cycle state transitions when a haul truck is in the loading area,
    including cycle initialization, loading operations, cycle closures, and
    special cases like implied loading and dump completion from load areas.

    The handler supports multiple operational scenarios:
    1. Normal loading: Explicit loading detection through continuous idling
    2. Implied loading: Loading that occurred between GPS updates
    3. Implied dumping: Dump operations completed from load area (DUMP_TIME)
    4. Outlier detection: Extended idle periods exceeding thresholds
    5. Cycle management: Closing previous cycles and creating new ones
    6. Continued loading: Trucks repositioning at same loader

    Key Features:
        - Detects loading start based on idle duration thresholds
        - Handles cycle closure for multiple segment transitions
        - Supports implied loading for low-frequency GPS updates
        - Manages implied dumping when DUMP_TIME trucks arrive at load area
        - Validates loader information and tracks loader positioning
        - Handles outlier detection with configurable thresholds
        - Stores loader information in tmp_idle_near_loader for future use

    Attributes:
        factory (CycleRecordFactory): Instance for creating cycle records
        calculator (DurationCalculator): Instance for time calculations

    Business Rules:
        - Truck must idle > IDLE_THRESHOLD_FOR_START_LOAD to start loading
        - Loading completes when truck transitions from IDLING to WORKING
        - Extended idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA marks as outlier
        - Extended idle > MAX_IDLE_THRESHOLD_TO_CLOSE_STATE triggers special
          cycle closure with synthetic durations
        - Implied operations handle GPS gaps and low update frequencies
        - Same loader validation uses DISTANCE_MAX_FOR_LOADER_MOVEMENT

    Example:
        >>> handler = LoadingAreaHandler()
        >>> updated, new = handler.handle(context)
        >>> if updated and new:
        ...     print(f"Closed cycle {updated.cycle_number} as {updated.cycle_status}")
        ...     print(f"Started new cycle {new.cycle_number}")
        >>> elif updated and updated.current_segment == CycleSegment.LOAD_TRAVEL:
        ...     print(f"Loading completed, truck now in LOAD_TRAVEL")
    """

    def __init__(self: Self) -> None:
        """Initialize the loading area handler.

        Sets up the factory and calculator instances used throughout
        the handler's operation.
        """
        self.factory = CycleRecordFactory()
        self.calculator = DurationCalculator()

    def handle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle cycle logic in loading area with multiple state patterns.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. Routes
        to specific handlers for different operational patterns.

        The method identifies three main patterns:
        1. Moving near loader: Both states WORKING (no action needed)
        2. Idling near loader: Both states IDLING (check thresholds)
        3. State transition: Different states (handle various transitions)

        Args:
            context (CycleComparisonContext): Contains current/previous records,
                loader information, and spatial context

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (None, None) if no action needed
                - (updated_record, None) for state updates
                - (updated_record, new_cycle) for cycle closures with new starts

        Note:
            The method delegates to specialized handlers based on detected
            patterns, ensuring appropriate business logic for each scenario.
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        if self._is_moving_near_loader(curr_rec, last_rec):
            return None, None
        elif self._is_idling_near_loader(curr_rec, last_rec):
            return self._handle_idling_near_loader(context)
        elif self._is_state_transition(curr_rec, last_rec):
            return self._handle_state_transition(context)

        return None, None

    def _is_moving_near_loader(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if truck is moving near loader.

        Determines if the truck is in a moving state near the loader
        based on both current and previous work states.

        Args:
            curr_rec: Current cycle record
            last_rec: Previous cycle record

        Returns:
            bool: True if truck is moving near loader, False otherwise
        """
        return (
            curr_rec.work_state_id == WorkState.WORKING
            and last_rec.current_work_state_id == WorkState.WORKING
        )

    def _is_idling_near_loader(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if truck is idling near loader.

        Determines if the truck is in an idle state near the loader
        based on both current and previous work states.

        Args:
            curr_rec: Current cycle record
            last_rec: Previous cycle record

        Returns:
            bool: True if truck is idling near loader, False otherwise
        """
        return (
            curr_rec.work_state_id == WorkState.IDLING
            and last_rec.current_work_state_id == WorkState.IDLING
        )

    def _is_state_transition(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if there's a state transition.

        Detects when the truck's work state has changed between
        the previous and current records.

        Args:
            curr_rec: Current cycle record
            last_rec: Previous cycle record

        Returns:
            bool: True if work state has changed, False otherwise
        """
        return curr_rec.work_state_id != last_rec.current_work_state_id

    def _handle_idling_near_loader(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle truck continuously idling near loader.

        Processes scenarios where a truck remains stationary near a loader.
        Determines whether to start loading, mark as outlier, or initiate
        a new cycle based on idle duration and current segment.

        The method handles:
        1. Initial idle: Check if threshold met to start operations
        2. LOAD_TIME segment: Check for outlier behavior
        3. Other segments: Initiate new cycle if threshold exceeded

        Args:
            context (CycleComparisonContext): Contains cycle and loader information

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (None, None) if idle duration below threshold
                - (updated_record, None) for outlier marking in LOAD_TIME
                - Results from _start_new_cycle for other scenarios

        Business Rules:
            - Uses IDLE_THRESHOLD_FOR_START_LOAD (default 20s) for loading start
            - Uses IDLE_THRESHOLD_IN_GEOFENCE_AREA (default 1200s) for outliers
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        idle_duration = self.calculator.calculate_idle_duration(
            previous_process_date=last_rec.current_process_date,
            current_process_date=curr_rec.timestamp
        )

        if idle_duration > IDLE_THRESHOLD_FOR_START_LOAD:
            if last_rec.current_segment == CycleSegment.LOAD_TIME.value:
                return self._load_time_idling_handler(context, idle_duration)
            return self._start_new_cycle(context)

        return None, None

    def _load_time_idling_handler(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle extended idling during LOAD_TIME segment.

        Processes scenarios where a truck already in LOAD_TIME continues
        idling beyond normal loading duration. Marks as outlier if idle
        time exceeds geofence threshold, indicating operational issues.

        Args:
            context (CycleComparisonContext): Contains current and previous records
            idle_duration (float): Total seconds truck has been idling

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (None, None) if already outlier or within threshold
                - (updated_record, None) with outlier marking if threshold exceeded

        Business Logic:
            - Skip if already marked as outlier
            - Mark as outlier if idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA
            - Captures loader position and sets outlier_type_id=3 for load area
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        loader_info = context.loader_asset

        if last_rec.is_outlier:
            return None, None
        else:
            if idle_duration > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )
                base_params.update({
                    "is_outlier": True,
                    "outlier_position_latitude": last_rec.current_asset_latitude,
                    "outlier_position_longitude": last_rec.current_asset_longitude,
                    "outlier_seconds": idle_duration,
                    "outlier_type_id": 3,
                    "outlier_date_utc": last_rec.current_process_date,
                    "outlier_loader_guid": loader_info.asset_guid,
                    "outlier_loader_latitude": loader_info.latitude,
                    "outlier_loader_longitude": loader_info.longitude,
                })
                updated_rec = CycleRecord(**base_params)
                return updated_rec, None
        return None, None

    def _start_new_cycle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Start a new cycle and close the previous one if needed.

        Handles cycle initialization and transitions based on the current
        segment. For initial records (no segment), sets up LOAD_TIME.
        For existing cycles, closes them appropriately and creates new ones.

        The method handles:
        1. Initial record (segment=None): Initialize LOAD_TIME segment
        2. Already in LOAD_TIME: No action needed
        3. Other segments: Close previous cycle and create new one

        Args:
            context (CycleComparisonContext): Contains cycle and loader information

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_initial_record, None) for cold start
                - (None, None) if already in LOAD_TIME
                - (closed_cycle, new_cycle) for segment transitions

        Raises:
            Logs error if loader information missing for initial record

        Note:
            Loader information is critical for initial records but may be
            available from context for subsequent cycles.
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        loader_rec = context.loader_asset
        load_reg = context.nearest_load_region

        if last_rec.current_segment is None:
            if loader_rec is None:
                logger.error(
                    f"[CORE ERROR] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Cannot start LOAD_TIME for initial record without "
                    "loader information"
                )
                return None, None

            # This is an initial record from cold start service
            # Update it to start LOAD_TIME segment
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            load_start = timestamp_to_utc_zero(last_rec.current_process_date)

            base_params.update({
                "current_segment": CycleSegment.LOAD_TIME.value,
                "loader_asset_guid": loader_rec.asset_guid,
                "loader_latitude": loader_rec.latitude,
                "loader_longitude": loader_rec.longitude,
                "previous_loader_distance": last_rec.current_loader_distance,
                "current_loader_distance": context.loader_distance,
                "load_start_utc": load_start,
                "created_date": last_rec.created_date,
                "load_region_guid": load_reg.region_guid if load_reg else None,
                "is_within_load_region": (
                    context.is_truck_within_load_region if load_reg else False
                ),
                "asset_load_region_distance": (
                    context.distance_truck_with_load_region if load_reg else None
                ),
            })

            logger.info(
                f"Starting LOAD_TIME for initial record "
                f"(cycle {last_rec.cycle_number})"
            )

            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

        elif last_rec.current_segment == CycleSegment.LOAD_TIME.value:
            return None, None

        else:
            # Existing cycle - close it and create new one
            updated_rec = self._close_previous_cycle(context)
            # Create new cycle
            new_params = self.factory.create_new_cycle_params(
                curr_rec, last_rec, context, loader_rec
            )
            new_cycle_rec = CycleRecord(**new_params)

            return updated_rec, new_cycle_rec

    def _close_previous_cycle(
        self: Self, context: CycleComparisonContext
    ) -> Optional[CycleRecord]:
        """Close the previous cycle based on its segment.

        Determines appropriate closure logic based on the segment the cycle
        was in when returning to the loading area. Different segments require
        different status assignments and duration calculations.

        Segment-specific handling:
        - LOAD_TRAVEL: Abnormal (truck dumped outside region) -> INVALID
        - EMPTY_TRAVEL: Normal completion -> COMPLETE or INVALID
        - DUMP_TIME: Unusual (skipped EMPTY_TRAVEL) -> closes with dump duration

        Args:
            context (CycleComparisonContext): Contains cycle transition information

        Returns:
            Optional[CycleRecord]: Updated record with cycle closure, or None
                if unexpected segment encountered

        Note:
            Each segment closure has specific business implications for
            operational tracking and compliance monitoring.
        """
        last_rec = context.last_record
        segment = last_rec.current_segment

        if segment == CycleSegment.LOAD_TRAVEL.value:
            return self._close_load_travel_cycle(context)
        elif segment == CycleSegment.EMPTY_TRAVEL.value:
            return self._close_empty_travel_cycle(context)
        elif segment == CycleSegment.DUMP_TIME.value:
            return self._close_dump_time_cycle(context)

        logger.warning(
            f"[CORE WARNING] {context.current_record.asset_guid} "
            f"{context.current_record.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"Unexpected segment {segment} when closing cycle "
            f"{last_rec.cycle_number}"
        )
        return None

    def _close_load_travel_cycle(
        self: Self, context: CycleComparisonContext
    ) -> CycleRecord:
        """Close a cycle that was in LOAD_TRAVEL when returning to loader.

        Handles abnormal cycle pattern where truck returns to loading area
        directly from LOAD_TRAVEL, indicating material was dumped outside
        designated dump regions. Always marks cycle as INVALID.

        Pattern: LOAD_TIME -> LOAD_TRAVEL -> LOAD_TIME (skipped DUMP_TIME)

        Args:
            context (CycleComparisonContext): Contains cycle information

        Returns:
            CycleRecord: Updated record with INVALID status and calculated
                load_travel_seconds

        Business Impact:
            - Identifies unauthorized dumping
            - Tracks material loss
            - Supports compliance monitoring
        """
        last_rec = context.last_record
        base_params = self.factory.create_base_params(
            last_rec, context.current_record
        )

        # Calculate load travel duration
        load_travel_seconds = self.calculator.calculate_seconds(
            last_rec.load_end_utc, last_rec.current_process_date
        )

        base_params.update({
            "cycle_status": CycleStatus.INVALID.value,
            "load_travel_seconds": load_travel_seconds,
            "previous_loader_distance": last_rec.current_loader_distance,
            "current_loader_distance": context.loader_distance,
            "cycle_end_utc": last_rec.current_process_date,
            "idle_in_dump_region_guid": None,
            "tmp_idle_near_loader": None,
        })

        logger.info(
            f"Closing INVALID cycle {last_rec.cycle_number}: "
            f"Abnormal transition from LOAD_TRAVEL to LOAD_TIME "
            f"(truck dumped outside dump region)"
        )

        return CycleRecord(**base_params)

    def _close_empty_travel_cycle(
        self: Self, context: CycleComparisonContext
    ) -> CycleRecord:
        """Close a cycle that was in EMPTY_TRAVEL when returning to loader.

        Handles normal cycle completion pattern where truck returns to
        loading area after completing all segments. Marks as COMPLETE if
        all segment durations present, otherwise INVALID.

        Pattern: LOAD_TIME -> LOAD_TRAVEL -> DUMP_TIME -> EMPTY_TRAVEL -> LOAD_TIME

        Args:
            context (CycleComparisonContext): Contains cycle information

        Returns:
            CycleRecord: Updated record with appropriate status and calculated
                empty_travel_seconds and total_cycle_seconds

        Business Logic:
            - Calculates empty_travel_seconds from dump_end to current
            - Validates all four segment durations exist
            - Sums durations for total_cycle_seconds if COMPLETE
        """
        last_rec = context.last_record
        base_params = self.factory.create_base_params(
            last_rec, context.current_record
        )

        # Calculate empty travel duration
        empty_travel_seconds = self.calculator.calculate_seconds(
            last_rec.dump_end_utc, last_rec.current_process_date
        )

        # Check if all required segments have valid durations
        has_all_segments = all([
            last_rec.load_seconds is not None,
            last_rec.load_travel_seconds is not None,
            last_rec.dump_seconds is not None,
            empty_travel_seconds is not None,
            last_rec.dump_end_utc is not None
        ])

        if has_all_segments:
            # Calculate total cycle time
            total_seconds = sum(filter(None, [
                last_rec.load_seconds,
                last_rec.load_travel_seconds,
                last_rec.dump_seconds,
                empty_travel_seconds
            ]))

            base_params.update({
                "cycle_status": CycleStatus.COMPLETE.value,
                "empty_travel_seconds": empty_travel_seconds,
                "total_cycle_seconds": total_seconds,
                "previous_loader_distance": last_rec.current_loader_distance,
                "current_loader_distance": context.loader_distance,
                "cycle_end_utc": last_rec.current_process_date,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })

            logger.info(
                f"Closing COMPLETE cycle {last_rec.cycle_number}: "
                f"Total time {total_seconds:.1f}s"
            )
        else:
            # Missing some segment durations
            base_params.update({
                "cycle_status": CycleStatus.INVALID.value,
                "empty_travel_seconds": empty_travel_seconds,
                "previous_loader_distance": last_rec.current_loader_distance,
                "current_loader_distance": context.loader_distance,
                "cycle_end_utc": last_rec.current_process_date,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })

            missing_segments = []
            if last_rec.load_seconds is None:
                missing_segments.append("load_seconds")
            if last_rec.load_travel_seconds is None:
                missing_segments.append("load_travel_seconds")
            if last_rec.dump_seconds is None:
                missing_segments.append("dump_seconds")

            logger.warning(
                f"[CORE WARNING] {context.current_record.asset_guid} "
                f"{context.current_record.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Closing INVALID cycle {last_rec.cycle_number}: "
                f"Missing segments: {', '.join(missing_segments)}"
            )

        return CycleRecord(**base_params)

    def _close_dump_time_cycle(
        self: Self,
        context: CycleComparisonContext
    ) -> CycleRecord:
        """Close a cycle that was in DUMP_TIME when returning to loader.

        Handles unusual pattern where truck returns to loading area directly
        from DUMP_TIME, skipping EMPTY_TRAVEL. Calculates dump duration based
        on idle time at dump location.

        Pattern: LOAD_TIME -> LOAD_TRAVEL -> DUMP_TIME -> LOAD_TIME

        Args:
            context (CycleComparisonContext): Contains cycle information

        Returns:
            CycleRecord: Updated record with dump duration and cycle closure

        Business Impact:
            - Identifies irregular operational patterns
            - May indicate emergency returns or disruptions
            - Cycle likely marked INVALID due to missing EMPTY_TRAVEL
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        base_params = self.factory.create_base_params(
            last_rec, context.current_record
        )
        dump_seconds = self.calculator.calculate_idle_duration(
            previous_process_date=last_rec.current_process_date,
            current_process_date=curr_rec.timestamp
        )
        base_params.update({
            "dump_region_guid": last_rec.idle_in_dump_region_guid,
            "dump_end_utc": curr_rec.timestamp,
            "dump_seconds": dump_seconds,
            "cycle_end_utc": curr_rec.timestamp,
            "idle_in_dump_region_guid": None,
            "tmp_idle_near_loader": None,
        })
        return CycleRecord(**base_params)

    def _handle_state_transition(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle work state transitions in loading area.

        Processes transitions between WORKING and IDLING states when the
        truck is in the loading area. This method handles both normal
        loading operations and implied loading scenarios where the truck
        completed loading between GPS updates.

        State Transitions:
            1. WORKING -> IDLING: Store loader info in tmp_idle_near_loader
            2. IDLING -> WORKING: Complex branching based on conditions:
               - Outlier recovery with synthetic loading
               - Implied dump completion (DUMP_TIME segment)
               - Implied loading for various segments
               - Normal loading completion
               - Simple state updates

        The implied loading logic handles real-world scenarios where:
            - GPS update frequency is low (30s to several minutes)
            - Network delays cause batched updates
            - Loading operations occur between tracking updates

        Args:
            context: Cycle comparison context containing:
                - current_record: Current state with work_state_id
                - last_record: Previous state with segment and timing
                - loader_asset: Required for implied loading scenarios
                - loader_distance: Current distance to loader

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - For state updates: (updated_record, None)
                - For cycle closure: (closed_cycle, new_cycle)
                - For no action: (None, None)

        Business Logic:
            The method implements sophisticated logic to handle real-world
            scenarios including GPS gaps, operational outliers, and
            segment-specific transitions. The tmp_idle_near_loader field
            stores loader information for future reference.

        Note:
            Implied loading creates more complete operational records
            even with sparse GPS data, improving KPI accuracy and
            operational insights.
        """
        curr_rec = context.current_record
        last_rec = context.last_record
        loader_info = context.loader_asset

        # WORKING -> IDLING transition (truck stops moving)
        if (
            last_rec.current_work_state_id == WorkState.WORKING
            and curr_rec.work_state_id == WorkState.IDLING
        ):
            # Just update the work state
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            load_info = {
                "loader_asset_guid": loader_info.asset_guid,
                "loader_latitude": loader_info.latitude,
                "loader_longitude": loader_info.longitude,
                "load_region_guid": context.nearest_load_region.region_guid,
                "is_within_load_region": context.is_truck_within_load_region,
                "asset_load_region_distance": (
                    context.distance_truck_with_load_region
                )
            }

            base_params.update({
                "tmp_idle_near_loader": json.dumps(load_info)
            })

            logger.debug(
                f"[CORE DEBUG] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "State transition WORKING->IDLING for cycle "
                f"{last_rec.cycle_number}"
            )

            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

        # IDLING -> WORKING transition (truck starts moving)
        elif (
            last_rec.current_work_state_id == WorkState.IDLING
            and curr_rec.work_state_id == WorkState.WORKING
        ):
            # Calculate idle duration for implied loading check
            idle_duration = self.calculator.calculate_idle_duration(
                previous_process_date=last_rec.current_process_date,
                current_process_date=curr_rec.timestamp
            )

            # handling previous record outlier
            if last_rec.is_outlier:
                return self._handle_outlier_load_completion(
                    context, idle_duration
                )

            if last_rec.current_segment == CycleSegment.DUMP_TIME.value:
                return self._handle_implied_dump_completion(context)

            # Check for implied loading completion
            elif idle_duration > IDLE_THRESHOLD_FOR_START_LOAD:
                # Determine if we should handle implied loading
                should_handle_implied = False

                if idle_duration > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
                    return self._handle_outlier_load_completion(
                        context, idle_duration
                    )
                elif last_rec.current_segment is None:
                    # Initial record scenario
                    should_handle_implied = True
                elif last_rec.current_segment == CycleSegment.EMPTY_TRAVEL.value:
                    # Normal cycle completion with implied loading
                    should_handle_implied = True
                elif last_rec.current_segment == CycleSegment.LOAD_TRAVEL.value:
                    # Condition where the previous record is IDLING in DUMP AREA
                    # or when the asset continue the loading process
                    should_handle_implied = True
                elif last_rec.current_segment not in [
                    CycleSegment.LOAD_TIME.value
                ]:
                    # Other unexpected segments at loader
                    # (DUMP_TIME is impossible due to geographic constraints)
                    should_handle_implied = True

                if should_handle_implied:
                    logger.debug(
                        f"[CORE DEBUG] {curr_rec.asset_guid} "
                        f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                        "Detected implied loading: truck was idle for "
                        f"{idle_duration:.1f}s (threshold: "
                        f"{IDLE_THRESHOLD_FOR_START_LOAD}s) and is now moving"
                    )
                    return self._handle_implied_load_completion(
                        context, idle_duration
                    )

            # Handle normal state transitions

            # If current_segment is None, just update work state
            if last_rec.current_segment is None:
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )

                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "State transition IDLING->WORKING for initial "
                    f"record (cycle {last_rec.cycle_number})"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

            # Check if completing normal LOAD_TIME
            elif (
                last_rec.current_segment == CycleSegment.LOAD_TIME.value
                and last_rec.load_start_utc is not None
            ):
                # Calculate loading duration
                load_end_time = timestamp_to_utc_zero(curr_rec.timestamp)
                load_seconds = self.calculator.calculate_seconds(
                    last_rec.load_start_utc,
                    load_end_time
                )

                # Update record with LOAD_TRAVEL segment
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )

                base_params.update({
                    "current_segment": CycleSegment.LOAD_TRAVEL.value,
                    "load_end_utc": load_end_time,
                    "load_seconds": load_seconds,
                    "previous_loader_distance": (
                        last_rec.current_loader_distance
                    ),
                    "current_loader_distance": context.loader_distance,
                })

                with open("/usr/src/app/debug.txt", "a") as f:
                    f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #715 | ADD LOAD SECONDS VALUE {load_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Completing LOAD_TIME for cycle "
                    f"{last_rec.cycle_number}: Load duration "
                    f"{load_seconds:.1f}s, transitioning to LOAD_TRAVEL"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

            else:
                # Just update the work state for other segments
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )

                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "State transition IDLING->WORKING for cycle "
                    f"{last_rec.cycle_number} in segment "
                    f"{last_rec.current_segment}"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

        # No transition detected
        return None, None

    def _handle_outlier_load_completion(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle cycle closure and creation after outlier idle at loader.

        Processes scenarios where a truck exceeded idle thresholds and needs
        to close the current cycle as OUTLIER and start a new cycle. Creates
        a synthetic 20-second loading period for the new cycle to maintain
        data continuity.

        The method handles recovery from extended idle periods that indicate
        operational issues like equipment failure, extended queuing, or other
        disruptions at the loading area.

        Args:
            context (CycleComparisonContext): Contains cycle and loader information
            idle_duration (float): Total seconds truck was idle before moving

        Returns:
            Tuple[CycleRecord, CycleRecord]:
                - Updated record: Previous cycle closed with OUTLIER status
                - New record: Fresh cycle started in LOAD_TRAVEL segment with
                  synthetic 20-second load time

        Business Logic:
            - Closes current cycle with OUTLIER status
            - Accumulates outlier_seconds if already marked as outlier
            - Creates new cycle with synthetic 20-second load duration
            - New cycle starts directly in LOAD_TRAVEL segment
            - Preserves loader information for operational tracking

        Note:
            The 20-second synthetic load time is a business rule ensuring
            all cycles have some loading duration while acknowledging the
            loading likely occurred during the untracked idle period.
        """
        curr_rec = context.current_record
        last_rec = context.last_record
        loader_rec = context.loader_asset

        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        base_params.update({
            "cycle_status": CycleStatus.OUTLIER.value,
            "cycle_end_utc": curr_rec.timestamp
        })

        if last_rec.is_outlier and last_rec.outlier_seconds:
            total_outlier_seconds = (
                last_rec.outlier_seconds + idle_duration
            )
            base_params.update({
                "outlier_seconds": total_outlier_seconds,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })
        else:
            base_params.update({
                "is_outlier": True,
                "outlier_seconds": idle_duration,
                "outlier_type_id": 3,
                "outlier_date_utc": last_rec.current_process_date,
                "outlier_loader_guid": loader_rec.asset_guid,
                "outlier_loader_latitude": loader_rec.latitude,
                "outlier_loader_longitude": loader_rec.longitude,
                "outlier_position_latitude": last_rec.current_asset_latitude,
                "outlier_position_longitude": last_rec.current_asset_longitude,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })

        new_params = self.factory.create_new_cycle_params(
            curr_rec, last_rec, context, loader_rec
        )

        load_start = curr_rec.timestamp - timedelta(seconds=20)
        load_seconds = self.calculator.calculate_seconds(
            start_time=load_start,
            end_time=curr_rec.timestamp
        )

        new_params.update({
            "current_segment": CycleSegment.LOAD_TRAVEL.value,
            "previous_work_state_id": last_rec.current_work_state_id,
            "previous_loader_distance": last_rec.current_loader_distance,
            "load_start_utc": load_start,
            "load_end_utc": curr_rec.timestamp,
            "load_seconds": load_seconds,
            "cycle_start_utc": curr_rec.timestamp,
        })

        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #838 | ADD LOAD SECONDS VALUE {load_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

        updated_rec = CycleRecord(**base_params)
        new_cycle_rec = CycleRecord(**new_params)

        return updated_rec, new_cycle_rec

    def _handle_implied_load_completion(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle case where truck loaded while idle but is now moving.

        Processes scenarios where a truck was idle at the loader long
        enough to complete loading (exceeded threshold) but the next GPS
        record shows it's already moving. This implies loading occurred
        during the idle period, requiring different handling based on
        the current segment.

        This method addresses real-world GPS tracking limitations where:
        - Update frequency is low (30s to several minutes)
        - Network issues cause delayed or batched updates
        - Loading operations complete between tracking points

        Segment-Specific Handling:
            1. None (initial): Update to LOAD_TRAVEL with loading info
            2. EMPTY_TRAVEL: Close previous cycle, create new in LOAD_TRAVEL
            3. LOAD_TRAVEL: Continue loading at same loader (repositioning)
               or handle dump area idling scenario
            4. Other: Log warning but handle gracefully (defensive programming)

        The LOAD_TRAVEL case handles two scenarios:
            a) Previous idle in DUMP area: Implies dump completed, transition
               to EMPTY_TRAVEL
            b) Continued loading: Truck repositioned at same loader (< 10m)

        Args:
            context: Cycle comparison context containing:
                - current_record: Shows truck is now WORKING
                - last_record: Shows truck was IDLING at loader
                - loader_asset: Required loader information
                - loader_distance: Distance to loader
            idle_duration: Total seconds truck was idle (already verified
                          to exceed IDLE_THRESHOLD_FOR_START_LOAD)

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - Initial: (updated_record, None)
                - EMPTY_TRAVEL: (closed_cycle, new_cycle)
                - LOAD_TRAVEL same loader: (updated_record, None)
                - LOAD_TRAVEL from dump: (updated_record, None)
                - Other: (updated_record, None) with warning

        Business Impact:
            - Captures loading operations missed by GPS gaps
            - Handles interrupted/repositioned loading scenarios
            - Maintains complete cycle records for KPI accuracy
            - Improves loader utilization tracking

        Example Timeline:
            Scenario 1 - Normal implied loading:
            10:00 - Truck arrives at loader (EMPTY_TRAVEL)
            10:00 - Truck idles (IDLING)
            10:05 - Truck departs (WORKING) - loading implied

            Scenario 2 - Continued loading:
            10:00 - Truck loading (LOAD_TIME)
            10:03 - Truck repositions (LOAD_TRAVEL)
            10:04 - Truck idles at same loader (IDLING)
            10:08 - Truck departs (WORKING) - continued loading
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        loader_rec = context.loader_asset
        load_reg = context.nearest_load_region

        # Handle based on current segment
        if last_rec.current_segment is None:
            # Initial record - update to LOAD_TRAVEL with implied loading
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            # Reconstruct loading times
            load_start = timestamp_to_utc_zero(last_rec.current_process_date)
            load_end = timestamp_to_utc_zero(curr_rec.timestamp)

            base_params.update({
                "current_segment": CycleSegment.LOAD_TRAVEL.value,
                "loader_asset_guid": loader_rec.asset_guid,
                "loader_latitude": loader_rec.latitude,
                "loader_longitude": loader_rec.longitude,
                "load_start_utc": load_start,
                "load_end_utc": load_end,
                "load_seconds": idle_duration,
                "previous_loader_distance": (
                    last_rec.current_loader_distance
                ),
                "current_loader_distance": context.loader_distance,
                "created_date": last_rec.created_date,
                "load_region_guid": load_reg.region_guid if load_reg else None,
                "is_within_load_region": (
                    context.is_truck_within_load_region if load_reg else False
                ),
                "asset_load_region_distance": (
                    context.distance_truck_with_load_region if load_reg else None
                ),
            })

            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #932 | ADD LOAD SECONDS VALUE {idle_duration}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

            logger.debug(
                f"[CORE DEBUG] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Implied LOAD_TIME completion for initial record: "
                f"Truck was idle {idle_duration:.1f}s at loader "
                f"{loader_rec.asset_guid} and is now in LOAD_TRAVEL"
            )

            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

        elif last_rec.current_segment == CycleSegment.EMPTY_TRAVEL.value:
            # Normal cycle completion with implied loading

            # Step 1: Close previous cycle
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            # Calculate empty travel duration
            empty_travel_seconds = None
            if last_rec.dump_end_utc:
                empty_travel_seconds = self.calculator.calculate_seconds(
                    last_rec.dump_end_utc,
                    last_rec.current_process_date  # When arrived at loader
                )

            # Determine cycle status
            if last_rec.is_outlier:
                cycle_status = CycleStatus.OUTLIER.value
            elif all([
                last_rec.load_seconds is not None,
                last_rec.load_travel_seconds is not None,
                last_rec.dump_seconds is not None,
                empty_travel_seconds is not None
            ]):
                cycle_status = CycleStatus.COMPLETE.value
            else:
                cycle_status = CycleStatus.INVALID.value

            # Calculate total cycle time
            total_seconds = None
            if cycle_status == CycleStatus.COMPLETE.value:
                total_seconds = sum([
                    last_rec.load_seconds or 0,
                    last_rec.load_travel_seconds or 0,
                    last_rec.dump_seconds or 0,
                    empty_travel_seconds or 0
                ])

            base_params.update({
                "cycle_status": cycle_status,
                "empty_travel_seconds": empty_travel_seconds,
                "total_cycle_seconds": total_seconds,
                "previous_loader_distance": (
                    last_rec.current_loader_distance
                ),
                "current_loader_distance": context.loader_distance,
                "cycle_end_utc": last_rec.current_process_date,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })

            updated_rec = CycleRecord(**base_params)

            # Step 2: Create new cycle with implied loading
            load_end = timestamp_to_utc_zero(curr_rec.timestamp)

            new_params = self.factory.create_new_cycle_params(
                curr_rec, last_rec, context, loader_rec
            )
            new_params.update({
                "current_segment": CycleSegment.LOAD_TRAVEL.value,
                "previous_work_state_id": (
                    last_rec.current_work_state_id
                ),
                "previous_loader_distance": (
                    last_rec.current_loader_distance
                ),
                "load_end_utc": load_end,
                "load_seconds": idle_duration
            })
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #1019 | ADD LOAD SECONDS VALUE {idle_duration}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")
            new_cycle_rec = CycleRecord(**new_params)

            logger.debug(
                f"[CORE DEBUG] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Closed {cycle_status} cycle {updated_rec.cycle_number} "
                f"and started cycle {new_cycle_rec.cycle_number} with "
                f"implied loading ({idle_duration:.1f}s)"
            )

            return updated_rec, new_cycle_rec
        # =============================================
        elif last_rec.current_segment == CycleSegment.LOAD_TRAVEL.value:
            # Truck in LOAD_TRAVEL returned to same loader for more loading
            # This handles interrupted or continued loading scenarios
            # Or if the asset is IDLING in DUMP AREA previously

            if last_rec.current_area == "DUMP":
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )
                load_travel_seconds = None
                if last_rec.load_end_utc:
                    load_travel_seconds = self.calculator.calculate_seconds(
                        start_time=last_rec.load_end_utc,
                        end_time=last_rec.current_process_date
                    )
                dump_start = timestamp_to_utc_zero(last_rec.current_process_date)
                dump_end = timestamp_to_utc_zero(curr_rec.timestamp)
                base_params.update({
                    "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                    "idle_in_dump_region_guid": None,
                    "dump_region_guid": last_rec.idle_in_dump_region_guid,
                    "dump_start_utc": dump_start,
                    "dump_end_utc": dump_end,
                    "dump_seconds": idle_duration,
                    "load_travel_seconds": load_travel_seconds
                })
                updated_rec = CycleRecord(**base_params)
                return updated_rec, None
            else:
                # Check if it's the same loader (within 10 meters)
                if (
                    last_rec.loader_latitude
                    and last_rec.loader_longitude
                    and (last_rec.loader_asset_guid == loader_rec.asset_guid)
                ):
                    # Calculate distance between previous and current loader
                    loader_dist_change = haversine_distance(
                        point1=[
                            last_rec.loader_latitude,
                            last_rec.loader_longitude
                        ],
                        point2=[
                            loader_rec.latitude,
                            loader_rec.longitude
                        ]
                    )

                    if (
                        (loader_dist_change <= DISTANCE_MAX_FOR_LOADER_MOVEMENT)
                        and (last_rec.current_area == "LOAD")
                    ):
                        # Same loader - continue loading
                        base_params = self.factory.create_base_params(
                            last_rec, curr_rec
                        )

                        # Update loading information
                        # Add idle duration to existing load time
                        total_load_seconds = (
                            (last_rec.load_seconds or 0) + idle_duration
                        )

                        base_params.update({
                            "previous_loader_distance": (
                                last_rec.current_loader_distance
                            ),
                            "current_loader_distance": context.loader_distance,
                            "load_end_utc": timestamp_to_utc_zero(
                                curr_rec.timestamp
                            ),
                            "load_seconds": total_load_seconds,
                        })

                        with open("/usr/src/app/debug.txt", "a") as f:
                            f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #1099 | ADD LOAD SECONDS VALUE {total_load_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

                        logger.debug(
                            f"[CORE DEBUG] {curr_rec.asset_guid} "
                            f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                            "Continued loading at same loader for cycle "
                            f"{last_rec.cycle_number}: Additional "
                            f"{idle_duration:.1f}s, total load time "
                            f"{total_load_seconds:.1f}s"
                        )

                        updated_rec = CycleRecord(**base_params)
                        return updated_rec, None
                    else:
                        # Different loader - log warning
                        logger.warning(
                            f"[CORE WARNING] {curr_rec.asset_guid} "
                            f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"In current record {curr_rec.timestamp}, "
                            "truck in LOAD_TRAVEL moved to different loader "
                            f"({loader_dist_change:.1f}m away) for cycle "
                            f"{last_rec.cycle_number}. Treating as state update."
                        )
                else:
                    # No previous loader coordinates - log warning
                    logger.warning(
                        f"[CORE WARNING] {curr_rec.asset_guid} "
                        f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                        "Cannot verify same loader for LOAD_TRAVEL continuation "
                        f"in cycle {last_rec.cycle_number}: missing previous "
                        "loader coordinates or the loader is not the same"
                    )

                # Fall through to default handling
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )
                updated_rec = CycleRecord(**base_params)
                return updated_rec, None
        # =============================================
        else:
            # Unexpected segment at loading area
            # This should not happen due to geographic constraints but
            # handle gracefully for defensive programming
            logger.warning(
                f"[CORE WARNING] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Unexpected implied loading from segment "
                f"{last_rec.current_segment} at loading area for cycle "
                f"{last_rec.cycle_number}. Only updating work state."
            )

            # Just update work state
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )
            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

    def _handle_implied_dump_completion(
        self: Self,
        context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle dump completion when DUMP_TIME truck arrives at load area.

        Processes the special case where a truck in DUMP_TIME segment arrives
        at the loading area, implying dump operations were completed. This
        method handles extended idle scenarios with synthetic duration assignment.

        The method implements special logic for extremely long idle periods
        (> MAX_IDLE_THRESHOLD_TO_CLOSE_STATE), assigning synthetic durations
        to maintain data integrity and close cycles appropriately.

        Args:
            context (CycleComparisonContext): Contains cycle information

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - If idle > MAX threshold: (closed_cycle, new_cycle) with
                  synthetic 900s dump and empty_travel durations
                - Otherwise: (updated_record, None) with calculated dump duration

        Business Logic:
            Extended idle (> 1800s default):
            - Assigns 900s dump_seconds
            - Assigns 900s empty_travel_seconds
            - Closes cycle (COMPLETE or INVALID based on segments)
            - Creates new cycle without segment

            Normal idle:
            - Calculates actual dump duration
            - Transitions to EMPTY_TRAVEL
            - Continues current cycle

        Note:
            The synthetic 900-second durations are business rules to handle
            edge cases where trucks have excessively long idle periods that
            likely indicate operational issues or data quality problems.
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        base_params.update({
            "current_segment": CycleSegment.EMPTY_TRAVEL.value,
            "idle_in_dump_region_guid": None,
        })

        idle_when_dump = self.calculator.calculate_idle_duration(
            previous_process_date=last_rec.dump_start_utc,
            current_process_date=curr_rec.timestamp
        )

        if idle_when_dump > MAX_IDLE_THRESHOLD_TO_CLOSE_STATE:
            dump_seconds = 900.0
            empty_travel_seconds = 900.0
            dump_end_utc = (
                last_rec.dump_start_utc + timedelta(seconds=900)
            )
            curr_rec_cycle_end_utc = (
                dump_end_utc + timedelta(seconds=900)
            )

            if all([
                last_rec.load_seconds is not None,
                last_rec.load_travel_seconds is not None,
                dump_seconds is not None,
                empty_travel_seconds is not None
            ]):
                cycle_status = CycleStatus.COMPLETE.value
            else:
                cycle_status = CycleStatus.INVALID.value

            total_seconds = None
            if cycle_status == CycleStatus.COMPLETE.value:
                total_seconds = sum([
                    last_rec.load_seconds or 0,
                    last_rec.load_travel_seconds or 0,
                    dump_seconds or 0,
                    empty_travel_seconds or 0
                ])

            base_params.update({
                "cycle_status": cycle_status,
                "dump_region_guid": last_rec.idle_in_dump_region_guid,
                "dump_seconds": dump_seconds,
                "empty_travel_seconds": empty_travel_seconds,
                "total_seconds": total_seconds,
                "dump_end_utc": dump_end_utc,
                "cycle_end_utc": curr_rec_cycle_end_utc,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })

            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #1243 | ADD DUMP SECONDS VALUE {dump_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

            new_params = self.factory.create_new_cycle_params(
                curr_rec, last_rec, context
            )

            new_params.update({
                "current_segment": None,
                "previous_work_state_id": last_rec.current_work_state_id,
                "current_loader_distance": None,
                "cycle_start_utc": curr_rec_cycle_end_utc,
            })

            updated_rec = CycleRecord(**base_params)
            new_cycle_rec = CycleRecord(**new_params)

        else:

            base_params.update({
                "dump_region_guid": last_rec.idle_in_dump_region_guid,
                "dump_seconds": idle_when_dump,
                "dump_end_utc": curr_rec.timestamp,
                "idle_in_dump_region_guid": None,
            })

            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | loading_area_handler.py #1272 | ADD DUMP SECONDS VALUE {idle_when_dump}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

            updated_rec = CycleRecord(**base_params)
            new_cycle_rec = None

        return updated_rec, new_cycle_rec
