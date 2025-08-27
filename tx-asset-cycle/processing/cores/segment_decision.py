"""Cycle segment decision module for tracking haul truck operations.

This module provides functionality to track and manage the operational cycles
of haul trucks in mining operations. It handles state transitions between
loading, dumping, and traveling areas, calculating cycle times and validating
cycle completeness.

Cycle Flow:
    Normal cycle progression:
        LOAD_TIME -> LOAD_TRAVEL -> DUMP_TIME -> EMPTY_TRAVEL -> LOAD_TIME

    Abnormal cycle (truck dumps outside designated dump region):
        LOAD_TIME -> LOAD_TRAVEL -> LOAD_TIME
        (skips DUMP_TIME and EMPTY_TRAVEL segments)

Cycle Status:
    - COMPLETE: All four segments executed with valid durations
    - INVALID: Missing segments or abnormal transitions
    - INPROGRESS: Cycle currently being executed
    - OUTLIER: Cycle terminated due to extended idle in travel segments

The main entry point is `cycle_records_comparison()` which processes cycle
comparison contexts and returns updated or new cycle records based on the
truck's current position and state.

Example:
    >>> context = CycleComparisonContext(
    ...     current_record=current,
    ...     last_record=last,
    ...     asset_position="loading_area"
    ... )
    >>> updated, new = cycle_records_comparison(context)
"""
import logging
from datetime import timedelta
from typing import Optional, Self, Tuple

from config.static_config import (DISTANCE_MAX_FOR_LOADER_MOVEMENT,
                                  IDLE_THRESHOLD_FOR_START_DUMP,
                                  IDLE_THRESHOLD_FOR_START_LOAD,
                                  IDLE_THRESHOLD_IN_GEOFENCE_AREA,
                                  IDLE_THRESHOLD_IN_TRAVEL_SEGMENT)
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import (DurationCalculator, haversine_distance,
                             timestamp_to_utc_zero)

from .identifiers import (CycleRecordFactory, CycleSegment, CycleStateHandler,
                          CycleStatus, WorkState)

logger = logging.getLogger(__name__)


class LoadingAreaHandler(CycleStateHandler):
    """Handler for loading area cycle logic with implied loading support.

    Manages cycle state transitions when a haul truck is in the loading
    area. This includes detecting when loading starts, tracking loading
    duration, handling transitions to travel segments, and managing
    implied loading scenarios where trucks complete loading between GPS
    updates.

    The handler supports both explicit loading detection (continuous
    idling) and implied loading detection (extended idle followed by
    movement), making it robust for sparse GPS data scenarios.

    Key Features:
        - Detects loading start based on idle duration thresholds
        - Handles cycle closure and new cycle creation
        - Supports implied loading for low-frequency GPS updates
        - Manages both normal and abnormal cycle transitions
        - Validates required loader information
        - Handles outlier detection for extended idle periods

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Business Rules:
        - Truck must idle > IDLE_THRESHOLD_FOR_START_LOAD to start loading
        - Loading completes when truck transitions from IDLING to WORKING
        - New cycles created after closing previous cycles
        - Implied loading handles cases where GPS missed loading state
        - Extended idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA marks as outlier

    Example:
        >>> handler = LoadingAreaHandler()
        >>> updated, new = handler.handle(context)
        >>> if updated and new:
        ...     # Previous cycle closed, new cycle started
        ...     print(f"Closed cycle {updated.cycle_number}")
        ...     print(f"Started cycle {new.cycle_number}")
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
        """Handle cycle logic in loading area.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. This
        includes handling trucks moving near loaders, idling at loaders,
        and state transitions.

        Args:
            context: Cycle comparison context with current/previous records

        Returns:
            Tuple of (updated_record, new_cycle_record) where either or
            both can be None

        Note:
            The method delegates to specific handlers based on the detected
            state pattern (moving, idling, or transitioning).
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
        """Handle truck idling near loader.

        Processes scenarios where a truck is stationary near a loader.
        If the truck has been idle for longer than the configured threshold
        and hasn't started loading yet, this initiates a new loading cycle.
        If already in LOAD_TIME and idle exceeds geofence threshold, marks
        as outlier.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record) or (None, None)

        Note:
            Uses IDLE_THRESHOLD_FOR_START_LOAD to determine when to
            start a new cycle and IDLE_THRESHOLD_IN_GEOFENCE_AREA for
            outlier detection.
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

        Processes scenarios where a truck is already in LOAD_TIME segment
        but continues idling beyond normal loading duration. If idle time
        exceeds the geofence area threshold, marks the record as outlier
        to indicate abnormal behavior.

        Args:
            context: Cycle comparison context containing current and last records
            idle_duration: Total seconds truck has been idling

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_record, None) if marking as outlier
                - (None, None) if no action needed

        Business Logic:
            - If already marked as outlier: No action taken
            - If idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA: Mark as outlier
            - Otherwise: Continue normal LOAD_TIME processing

        Note:
            Outlier detection helps identify trucks that may be broken down,
            waiting excessively, or experiencing operational issues.
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        if last_rec.is_outlier:
            return None, None
        else:
            if idle_duration > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )
                base_params.update({
                    "is_outlier": True,
                    "outlier_position_latitude": curr_rec.latitude,
                    "outlier_position_longitude": curr_rec.longitude,
                    "outlier_seconds": idle_duration
                })
                updated_rec = CycleRecord(**base_params)
                return updated_rec, None
        return None, None

    def _start_new_cycle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Start a new cycle and close the previous one if needed.

        Handles two scenarios:
        1. If last_rec.current_segment is None: Updates the initial record
           from cold start to begin LOAD_TIME segment
        2. If last_rec.current_segment exists: Closes the previous cycle
           and creates a new one

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record) where:
            - For initial records: (updated_initial_record, None)
            - For existing cycles: (closed_previous_cycle, new_cycle)

        Raises:
            Logs error if loader information is missing when required
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        loader_rec = context.loader_asset

        if last_rec.current_segment is None:
            if loader_rec is None:
                logger.error(
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

        Determines how to close a cycle based on what segment it was in.
        Different segments require different closing logic:

        - LOAD_TRAVEL: Indicates abnormal transition (truck dumped outside
          dump region), cycle marked as INVALID
        - EMPTY_TRAVEL: Normal transition, can be COMPLETE or INVALID based
          on whether all required segments were executed
        - DUMP_TIME: Transition from dump area back to load area without
          completing empty travel

        Args:
            context: Cycle comparison context

        Returns:
            Optional[CycleRecord]: Updated record with closed cycle,
                                 or None if no closing needed

        Note:
            This method is called when a truck has been idle long enough
            to start a new loading cycle, requiring the previous cycle
            to be properly closed.
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
            f"Unexpected segment {segment} when closing cycle "
            f"{last_rec.cycle_number}"
        )
        return None

    def _close_load_travel_cycle(
        self: Self, context: CycleComparisonContext
    ) -> CycleRecord:
        """Close a load travel cycle (abnormal transition).

        Closes a cycle that was in the LOAD_TRAVEL segment when returning
        to LOAD_TIME. This indicates an abnormal transition where the truck
        dumped material outside the designated dump region, skipping the
        DUMP_TIME and EMPTY_TRAVEL segments. The cycle is always marked
        as INVALID.

        This handles the abnormal pattern:
        LOAD_TIME -> LOAD_TRAVEL -> LOAD_TIME (skipping DUMP_TIME & EMPTY_TRAVEL)

        Args:
            context: Cycle comparison context

        Returns:
            CycleRecord: Updated record with INVALID status and calculated
                        load travel duration

        Business Impact:
            - Identifies unauthorized dumping locations
            - Tracks material loss outside designated areas
            - Helps improve operational compliance
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
        """Close an empty travel cycle (normal transition).

        Closes a cycle that was in the EMPTY_TRAVEL segment when returning
        to LOAD_TIME. This is the normal cycle completion pattern where all
        segments were executed in order:
        LOAD_TIME -> LOAD_TRAVEL -> DUMP_TIME -> EMPTY_TRAVEL -> LOAD_TIME

        The cycle is marked as:
        - COMPLETE: If all required segments have valid durations
        - INVALID: If any required segment duration is missing

        Args:
            context: Cycle comparison context

        Returns:
            CycleRecord: Updated record with appropriate status and
                        calculated durations

        Note:
            A cycle is considered complete only if it has valid durations
            for all four segments: loading, load travel, dumping, and
            empty travel.
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
            })

            missing_segments = []
            if last_rec.load_seconds is None:
                missing_segments.append("load_seconds")
            if last_rec.load_travel_seconds is None:
                missing_segments.append("load_travel_seconds")
            if last_rec.dump_seconds is None:
                missing_segments.append("dump_seconds")

            logger.warning(
                f"Closing INVALID cycle {last_rec.cycle_number}: "
                f"Missing segments: {', '.join(missing_segments)}"
            )

        return CycleRecord(**base_params)

    def _close_dump_time_cycle(
        self: Self,
        context: CycleComparisonContext
    ) -> CycleRecord:
        """Close a dump time cycle (unusual transition).

        Closes a cycle that was in the DUMP_TIME segment when returning
        to LOAD_TIME. This is an unusual pattern where the truck returns
        to loading without completing the EMPTY_TRAVEL segment, indicating
        either operational issues or special circumstances.

        Pattern handled:
        LOAD_TIME -> LOAD_TRAVEL -> DUMP_TIME -> LOAD_TIME (skipping EMPTY_TRAVEL)

        Args:
            context: Cycle comparison context with current and last records

        Returns:
            CycleRecord: Updated record with calculated dump duration and
                        cycle end time. The cycle will likely be marked
                        INVALID due to missing EMPTY_TRAVEL segment.

        Business Impact:
            - Identifies irregular operational patterns
            - May indicate emergency returns or operational disruptions
            - Helps track cycle inefficiencies

        Note:
            This scenario is rare but must be handled to maintain data
            integrity and avoid orphaned DUMP_TIME segments.
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
            "idle_in_dump_region_guid": None,
            "dump_region_guid": last_rec.idle_in_dump_region_guid,
            "dump_end_utc": curr_rec.timestamp,
            "dump_seconds": dump_seconds,
            "cycle_end_utc": curr_rec.timestamp,
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

        State Transitions Handled:
            1. WORKING -> IDLING: Truck stops moving, only updates state
            2. IDLING -> WORKING: Truck starts moving
            - If idle > threshold: Implies loading occurred, handles
                based on current segment
            - If in LOAD_TIME with load_start_utc: Completes loading
                normally, transitions to LOAD_TRAVEL
            - Otherwise: Only updates work state

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

        Note:
            Implied loading creates more complete operational records
            even with sparse GPS data, improving KPI accuracy and
            operational insights.
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        # WORKING -> IDLING transition (truck stops moving)
        if (
            last_rec.current_work_state_id == WorkState.WORKING
            and curr_rec.work_state_id == WorkState.IDLING
        ):
            # Just update the work state
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            logger.debug(
                f"State transition WORKING->IDLING for cycle "
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

            # Check for implied loading completion
            if idle_duration > IDLE_THRESHOLD_FOR_START_LOAD:
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
                        f"Detected implied loading: truck was idle for "
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
                    f"State transition IDLING->WORKING for initial "
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

                logger.debug(
                    f"Completing LOAD_TIME for cycle "
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
                    f"State transition IDLING->WORKING for cycle "
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
        """Handle cycle closure and creation after outlier idle period.

        Processes scenarios where a truck exceeded idle thresholds and needs
        to close the current cycle as OUTLIER and start a new cycle. This
        handles recovery from extended idle periods that indicate operational
        issues.

        The method creates a synthetic loading period of 20 seconds for the
        new cycle to maintain data continuity, as the actual loading likely
        occurred during the extended idle period.

        Args:
            context: Cycle comparison context with current/previous records
            idle_duration: Total seconds truck was idle before moving

        Returns:
            Tuple[CycleRecord, CycleRecord]:
                - Updated record: Previous cycle closed with OUTLIER status
                - New record: Fresh cycle started in LOAD_TRAVEL segment

        Business Logic:
            - Closes current cycle with OUTLIER status
            - Accumulates outlier seconds if already marked as outlier
            - Creates new cycle with synthetic 20-second load time
            - New cycle starts directly in LOAD_TRAVEL segment

        Note:
            The 20-second synthetic load time is a business rule to ensure
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
            })
        else:
            base_params.update({
                "is_outlier": True,
                "outlier_seconds": idle_duration,
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
            "loader_asset_guid": None,
            "loader_latitude": None,
            "previous_loader_distance": last_rec.current_loader_distance,
            "load_start_utc": load_start,
            "load_end_utc": curr_rec.timestamp,
            "load_seconds": load_seconds,
            "cycle_start_utc": curr_rec.timestamp,
        })

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
                - Initial record: (updated_record, None)
                - EMPTY_TRAVEL: (closed_cycle, new_cycle)
                - LOAD_TRAVEL at same loader: (updated_record, None)
                - LOAD_TRAVEL from dump: (updated_record, None)
                - Unexpected segments: (updated_record, None) with warning
                - Missing loader: (None, None) with error

        Business Impact:
            - Captures loading operations that occur between updates
            - Handles interrupted/continued loading scenarios
            - Maintains complete cycle records for accurate KPIs
            - Improves loader utilization tracking

        Example Timeline:
            10:00 - Truck starts loading (LOAD_TIME)
            10:03 - Truck moves to reposition (LOAD_TRAVEL)
            10:04 - Truck idles at same loader (IDLING)
            10:08 - Truck departs (WORKING) - continued loading detected
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        loader_rec = context.loader_asset

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
                "created_date": last_rec.created_date,  # Preserve original
            })

            logger.debug(
                f"Implied LOAD_TIME completion for initial record: "
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
            new_cycle_rec = CycleRecord(**new_params)

            logger.debug(
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

                    if loader_dist_change <= DISTANCE_MAX_FOR_LOADER_MOVEMENT:
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

                        logger.debug(
                            f"Continued loading at same loader for cycle "
                            f"{last_rec.cycle_number}: Additional "
                            f"{idle_duration:.1f}s, total load time "
                            f"{total_load_seconds:.1f}s"
                        )

                        updated_rec = CycleRecord(**base_params)
                        return updated_rec, None
                    else:
                        # Different loader - log warning
                        logger.warning(
                            f"Truck in LOAD_TRAVEL moved to different loader "
                            f"({loader_dist_change:.1f}m away) for cycle "
                            f"{last_rec.cycle_number}. Treating as state update."
                        )
                else:
                    # No previous loader coordinates - log warning
                    logger.warning(
                        f"Cannot verify same loader for LOAD_TRAVEL continuation "
                        f"in cycle {last_rec.cycle_number}: missing previous "
                        f"loader coordinates"
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
                f"Unexpected implied loading from segment "
                f"{last_rec.current_segment} at loading area for cycle "
                f"{last_rec.cycle_number}. Only updating work state."
            )

            # Just update work state
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )
            updated_rec = CycleRecord(**base_params)
            return updated_rec, None


class DumpingAreaHandler(CycleStateHandler):
    """Handler for dumping area cycle logic with implied dumping support.

    Manages cycle state transitions when a haul truck is in the dumping area.
    This includes detecting when dumping starts, tracking dumping duration,
    handling transitions to empty travel segments, and managing implied dumping
    scenarios where trucks complete dumping between GPS updates.

    The handler supports both explicit dumping detection (continuous idling
    leading to DUMP_TIME) and implied dumping detection (extended idle followed
    by movement), making it robust for sparse GPS data scenarios.

    Key Features:
        - Detects dumping start based on idle duration thresholds
        - Transitions from LOAD_TRAVEL to DUMP_TIME when threshold exceeded
        - Handles implied dumping for low-frequency GPS updates
        - Manages dump completion and transition to EMPTY_TRAVEL
        - Supports continued dumping at same region (repositioning)
        - Records dump region information for tracking
        - Detects outlier behavior for extended idle periods

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Business Rules:
        - Truck must idle > IDLE_THRESHOLD_FOR_START_DUMP to start dumping
        - Dumping completes when truck transitions from IDLING to WORKING
        - Implied dumping handles cases where GPS missed dumping state
        - Same dump region validated for continued dumping scenarios
        - Extended idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA marks as outlier

    Example:
        >>> handler = DumpingAreaHandler()
        >>> updated, _ = handler.handle(context)
        >>> if updated and updated.current_segment == "DUMP_TIME":
        ...     print(f"Started dumping at region {updated.dump_region_guid}")

    Note:
        Unlike LoadingAreaHandler, DumpingAreaHandler typically doesn't create
        new cycles except when recovering from outlier status. It primarily
        updates existing cycles with dumping information.
    """

    def __init__(self: Self) -> None:
        """Initialize the dumping area handler.

        Sets up the factory and calculator instances used throughout
        the handler's operation.
        """
        self.factory = CycleRecordFactory()
        self.calculator = DurationCalculator()

    def handle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle cycle logic in dumping area.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. This
        includes handling trucks idling for dumping and state transitions.

        The method routes to specific handlers based on detected patterns:
        - Continuous idling: May trigger DUMP_TIME segment
        - State transitions: Handle start/stop of dumping operations

        Args:
            context: Cycle comparison context with current/previous records
                    and dump_region information

        Returns:
            Tuple of (updated_record, new_record) - typically doesn't create
            new cycles in dumping area. The second element is None unless
            recovering from outlier status.

        Note:
            The dumping area handler only updates existing records,
            except when creating new cycles after outlier recovery.
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        if self._is_continuous_idling(curr_rec, last_rec):
            return self._handle_continuous_idling(context)
        elif self._is_state_transition(curr_rec, last_rec):
            return self._handle_state_transition(context)

        return None, None

    def _is_continuous_idling(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if truck is continuously idling.

        Determines if the truck is continuously idling in the dump area
        based on both current and previous work states.

        Args:
            curr_rec: Current realtime record
            last_rec: Previous cycle record

        Returns:
            bool: True if continuously idling, False otherwise
        """
        return (
            last_rec.current_work_state_id == WorkState.IDLING
            and curr_rec.work_state_id == WorkState.IDLING
        )

    def _is_state_transition(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if there's a state transition.

        Detects when the truck's work state has changed between
        the previous and current records.

        Args:
            curr_rec: Current realtime record
            last_rec: Previous cycle record

        Returns:
            bool: True if work state has changed, False otherwise
        """
        return curr_rec.work_state_id != last_rec.current_work_state_id

    def _handle_continuous_idling(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle truck continuously idling in dump area.

        Processes scenarios where a truck is continuously stationary in
        the dump area. If the truck has been idle for longer than the
        configured threshold, it transitions to DUMP_TIME segment or
        continues accumulating dump time if already dumping.

        The method calculates idle duration and delegates to threshold
        handler if the duration exceeds IDLE_THRESHOLD_FOR_START_DUMP.

        Args:
            context: Cycle comparison context containing:
                    - current_record: Shows truck is still IDLING
                    - last_record: Shows truck was IDLING
                    - dump_region: Required for dump tracking

        Returns:
            Tuple of (updated_record, None) or (None, None)
                - Updated record if idle threshold exceeded
                - (None, None) if still within threshold

        Business Logic:
            - Idle duration calculated from last to current process date
            - Threshold check determines if dumping should start
            - Already dumping trucks continue accumulating time

        Note:
            Uses IDLE_THRESHOLD_FOR_START_DUMP (typically 3 minutes) to
            determine when to start dumping. This prevents false positives
            from brief stops.
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Calculate idle duration
        idle_duration = self.calculator.calculate_idle_duration(
            previous_process_date=last_rec.current_process_date,
            current_process_date=curr_rec.timestamp
        )

        if idle_duration > IDLE_THRESHOLD_FOR_START_DUMP:
            return self._handle_idle_threshold_exceeded(context, idle_duration)

        # Not idle long enough, no action needed
        return None, None

    def _handle_idle_threshold_exceeded(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle when idle threshold is exceeded in dump area.

        Determines appropriate action based on current segment and idle
        duration when the idle threshold for dumping has been exceeded.
        This method now includes outlier detection for extended idle periods
        that exceed operational norms.

        The method follows a priority order:
        1. Check if already marked as outlier (no further action)
        2. Check if idle exceeds geofence threshold (mark as outlier)
        3. Otherwise, handle normal dumping transitions

        Segment-specific behavior for normal transitions:
        - LOAD_TRAVEL: Transition to DUMP_TIME and calculate load travel time
        - DUMP_TIME: Already dumping, no action needed
        - Other: Transition to DUMP_TIME (unusual but handled)

        Args:
            context: Cycle comparison context with segment information
            idle_duration: Total seconds truck has been idling

        Returns:
            Tuple of (updated_record, None) or (None, None)
                - Updated record for segment transitions or outlier marking
                - (None, None) if already in DUMP_TIME or already outlier

        Business Impact:
            - Ensures accurate dump time tracking
            - Identifies trucks with excessive idle time at dump
            - Calculates load travel duration when appropriate
            - Handles edge cases gracefully

        Note:
            The outlier check takes precedence over normal segment transitions
            to ensure problematic behavior is captured. The DUMP_TIME check
            prevents duplicate processing when a truck continues idling at
            the dump location.
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        segment = last_rec.current_segment

        if last_rec.is_outlier:
            return None, None
        elif idle_duration > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )
            base_params.update({
                "is_outlier": True,
                "outlier_position_latitude": curr_rec.latitude,
                "outlier_position_longitude": curr_rec.longitude,
                "outlier_seconds": idle_duration
            })
            updated_rec = CycleRecord(**base_params)
            return updated_rec, None
        else:
            if segment == CycleSegment.DUMP_TIME.value:
                # Already in DUMP_TIME, no action needed
                logger.debug(
                    f"Already in DUMP_TIME for cycle {last_rec.cycle_number}"
                )
                return None, None

            elif segment == CycleSegment.LOAD_TRAVEL.value:
                # Transition from LOAD_TRAVEL to DUMP_TIME
                return self._transition_load_travel_to_dump_time(context)

            else:
                # Other segments transitioning to DUMP_TIME
                return self._transition_to_dump_time(context)

    def _transition_load_travel_to_dump_time(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, None]:
        """Transition from LOAD_TRAVEL to DUMP_TIME segment.

        Updates the cycle record to DUMP_TIME segment when a truck that was
        traveling with load has been idle long enough to start dumping.
        This is the normal progression in the cycle flow.

        The method:
        1. Validates dump region information is available
        2. Calculates load travel duration from load_end_utc
        3. Sets dump_start_utc to when idling began
        4. Updates segment to DUMP_TIME with dump region info

        Args:
            context: Cycle comparison context containing:
                    - last_record: With LOAD_TRAVEL segment and timing
                    - current_record: Current position information
                    - dump_region: Required dump region information

        Returns:
            Tuple of (updated_record, None)
                - Updated record with DUMP_TIME segment and calculations
                - None (never creates new cycles)

        Raises:
            Logs error and returns (None, None) if dump_region is missing

        Business Logic:
            - Load travel time = dump arrival - load completion
            - Dump starts when truck became idle (not current time)
            - Dump region tracked for operational reporting

        Example Timeline:
            09:45 - Load completed (load_end_utc)
            09:55 - Arrived at dump, started idling
            09:58 - Still idling (current), threshold exceeded
            Result: load_travel_seconds = 600s (10 min)
                   dump_start_utc = 09:55
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        dump_region = context.dump_region

        if not dump_region:
            logger.error(
                f"Cannot start DUMP_TIME without dump region information "
                f"for cycle {last_rec.cycle_number}"
            )
            return None, None

        # Create base parameters
        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        # Calculate load travel duration
        load_travel_seconds = None
        if last_rec.load_end_utc is not None:
            load_travel_seconds = self.calculator.calculate_seconds(
                last_rec.load_end_utc,
                last_rec.current_process_date
            )

        # Set dump start time
        dump_start = timestamp_to_utc_zero(last_rec.current_process_date)

        # Update parameters for DUMP_TIME
        base_params.update({
            "current_segment": CycleSegment.DUMP_TIME.value,
            "dump_region_guid": dump_region.region_guid,
            "dump_start_utc": dump_start,
            "load_travel_seconds": load_travel_seconds,
        })

        logger.info(
            f"Transitioning from LOAD_TRAVEL to DUMP_TIME for cycle "
            f"{last_rec.cycle_number} at region {dump_region.region_guid}"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _transition_to_dump_time(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, None]:
        """Transition to DUMP_TIME from unexpected segments.

        Updates the cycle record to DUMP_TIME segment for segments other
        than LOAD_TRAVEL. This handles edge cases where trucks end up at
        dump areas from unexpected states.

        This is an unusual scenario that might occur due to:
        - GPS data gaps causing missed state transitions
        - Operational irregularities
        - Initial records starting at dump area

        Args:
            context: Cycle comparison context with dump region

        Returns:
            Tuple of (updated_record, None)
                - Updated record with DUMP_TIME segment
                - None (never creates new cycles)

        Raises:
            Logs error and returns (None, None) if dump_region is missing

        Warning:
            Logs warning about unexpected segment transition to help
            identify operational anomalies or data quality issues.

        Note:
            While unusual, this transition is handled to maintain data
            integrity and avoid stuck cycles. The resulting cycle may
            be marked INVALID due to missing expected segments.
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        dump_region = context.dump_region

        if not dump_region:
            logger.error(
                f"Cannot start DUMP_TIME without dump region information "
                f"for cycle {last_rec.cycle_number}"
            )
            return None, None

        # Create base parameters
        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        # Set dump start time
        dump_start = timestamp_to_utc_zero(last_rec.current_process_date)

        # Update parameters for DUMP_TIME
        base_params.update({
            "current_segment": CycleSegment.DUMP_TIME.value,
            "dump_region_guid": dump_region.region_guid,
            "dump_start_utc": dump_start,
        })

        logger.warning(
            f"Starting DUMP_TIME from unexpected segment "
            f"{last_rec.current_segment} for cycle {last_rec.cycle_number}"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _handle_state_transition(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle work state transitions in dumping area.

        Processes transitions between WORKING and IDLING states when the
        truck is in the dumping area. This method handles normal dumping
        operations, implied dumping scenarios, and outlier recovery cases
        where trucks exceed operational thresholds.

        State Transitions Handled:
            1. WORKING -> IDLING: Truck stops moving, only updates work state
               and sets idle_in_dump_region_guid
            2. IDLING -> WORKING: Truck starts moving
               - If outlier or idle > geofence threshold: Handle outlier
                 recovery (close cycle, create new)
               - If idle > dump threshold: Implies dumping occurred,
                 transitions directly to EMPTY_TRAVEL
               - If already in DUMP_TIME: Completes dumping, transitions
                 to EMPTY_TRAVEL
               - Otherwise: Only updates work state

        Args:
            context: Cycle comparison context containing:
                - current_record: Current state with work_state_id
                - last_record: Previous state with timing information
                - dump_region: Dump region information (if available)

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_record, None) for normal transitions
                - (updated_record, new_cycle) for outlier recovery
                - (None, None) if no action needed

        Note:
            The method now includes outlier detection and recovery, allowing
            it to create new cycles when recovering from extended idle periods.
            This ensures continuous tracking despite operational disruptions.

        Example:
            If a truck was idle for 5 minutes (> 3 minute threshold) but
            the next GPS update shows it's already moving, we infer that
            dumping occurred during the idle period and transition directly
            to EMPTY_TRAVEL.
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        # WORKING -> IDLING transition (truck stops moving)
        if (
            last_rec.current_work_state_id == WorkState.WORKING
            and curr_rec.work_state_id == WorkState.IDLING
        ):
            # Just update the work state
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            base_params.update({
                "idle_in_dump_region_guid": (
                    context.dump_region.region_guid
                )
            })

            logger.debug(
                f"State transition WORKING->IDLING in dump area "
                f"for cycle {last_rec.cycle_number}"
            )

            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

        # IDLING -> WORKING transition (truck starts moving)
        elif (
            last_rec.current_work_state_id == WorkState.IDLING
            and curr_rec.work_state_id == WorkState.WORKING
        ):
            # Check if idle duration exceeded dump threshold
            idle_duration = self.calculator.calculate_idle_duration(
                previous_process_date=last_rec.current_process_date,
                current_process_date=curr_rec.timestamp
            )

            if last_rec.is_outlier or idle_duration > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
                return self._handle_outlier_dump_completion(context, idle_duration)

            if idle_duration > IDLE_THRESHOLD_FOR_START_DUMP:
                # Truck was idle long enough to dump but is now moving
                # Need to handle the implied DUMP_TIME → EMPTY_TRAVEL
                return self._handle_implied_dump_completion(
                    context, idle_duration
                )

            # Check if completing DUMP_TIME normally
            elif last_rec.current_segment == CycleSegment.DUMP_TIME.value:
                return self._complete_dump_time_to_empty_travel(context)
            else:
                # Just update the work state
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )

                logger.debug(
                    f"State transition IDLING->WORKING in dump area "
                    f"for cycle {last_rec.cycle_number}"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

        # No transition detected
        return None, None

    def _handle_outlier_dump_completion(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle cycle closure and creation after outlier idle at dump.

        Processes scenarios where a truck exceeded idle thresholds at the
        dump area and needs to close the current cycle as OUTLIER and start
        a new cycle. This handles recovery from extended idle periods that
        indicate operational issues at the dumping location.

        Unlike the loading area handler, this method doesn't create synthetic
        dump times as the truck may have had issues preventing normal dumping
        operations. The new cycle starts fresh without any segment.

        Args:
            context: Cycle comparison context with current/previous records
            idle_duration: Total seconds truck was idle at dump area

        Returns:
            Tuple[CycleRecord, CycleRecord]:
                - Updated record: Previous cycle closed with OUTLIER status
                - New record: Fresh cycle started with no segment

        Business Logic:
            - Closes current cycle with OUTLIER status
            - Accumulates outlier seconds if already marked as outlier
            - Creates new cycle starting from no segment
            - Preserves cycle continuity despite disruptions

        Note:
            This is a new method added to handle outlier scenarios specific
            to the dump area. It ensures proper cycle management when trucks
            have extended idle periods that exceed operational norms.

        Example Timeline:
            10:00 - Truck arrives at dump area
            10:05 - Truck starts idling (equipment issue)
            11:35 - Still idling (90 min > geofence threshold)
            11:40 - Truck resumes operation
            Result: Previous cycle closed as OUTLIER, new cycle created
        """
        curr_rec = context.current_record
        last_rec = context.last_record

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
            })
        else:
            base_params.update({
                "is_outlier": True,
                "outlier_seconds": idle_duration,
            })

        new_params = self.factory.create_new_cycle_params(
            curr_rec, last_rec, context
        )

        new_params.update({
            "current_segment": None,
            "previous_work_state_id": last_rec.current_work_state_id,
            "cycle_start_utc": curr_rec.timestamp,
        })

        updated_rec = CycleRecord(**base_params)
        new_cycle_rec = CycleRecord(**new_params)

        return updated_rec, new_cycle_rec

    def _handle_implied_dump_completion(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[CycleRecord, None]:
        """Handle case where truck dumped while idle but is now moving.

        Processes the scenario where a truck was idle in the dump area
        long enough to complete dumping (exceeded threshold) but the next
        GPS record shows it's already moving. This implies dumping occurred
        during the idle period, so we transition directly to EMPTY_TRAVEL
        and calculate all relevant durations.

        This method handles the edge case where GPS update frequency is
        low and we don't capture the intermediate state where the truck
        was actively dumping (IDLING in DUMP_TIME segment).

        Segment-Specific Behavior:
            - LOAD_TRAVEL: Normal flow, calculates load_travel_seconds
              from load_end_utc to last_rec.current_process_date
            - EMPTY_TRAVEL: Continued dumping at same region (repositioning)
              Validates same dump region before accumulating dump time
            - None (initial): Transitions but likely creates INVALID cycle
            - Other segments: Logs warning but still transitions

        The EMPTY_TRAVEL case handles interrupted dumping where trucks:
            - Move slightly to reposition during dumping
            - Have dumping interrupted and resumed
            - Need multiple dumping positions at the same region
        This is validated by checking the dump region hasn't changed.

        Time Calculations:
            - dump_start_utc: When truck became idle (last_rec.current_process_date)
            - dump_end_utc: Current time (when truck is moving)
            - dump_seconds: Duration of idle period (idle_duration)
            - load_travel_seconds: From load_end_utc to arrival at dump
              (only if load_end_utc exists)

        Args:
            context: Cycle comparison context containing:
                - current_record: Shows truck is now WORKING
                - last_record: Shows truck was IDLING in dump area
                - dump_region: Required dump region information
            idle_duration: Total seconds truck was idle (already calculated
                          and verified to exceed IDLE_THRESHOLD_FOR_START_DUMP)

        Returns:
            Tuple[CycleRecord, None]: Updated record with:
                - current_segment set to EMPTY_TRAVEL
                - dump_region_guid set
                - dump_start_utc set to when idling began
                - dump_end_utc set to current time
                - dump_seconds set to idle_duration
                - load_travel_seconds calculated (if possible)
                - idle_in_dump_region_guid cleared
            Returns (None, None) if dump_region is missing

        Example Timeline:
            Case 1 - Normal dump:
            10:00 - Truck arrives at dump (LOAD_TRAVEL)
            10:00 - Truck idles (IDLING)
            10:05 - Truck moving (WORKING) - dump complete

            Case 2 - Continued dump:
            10:00 - Truck dumps partially (DUMP_TIME)
            10:03 - Truck repositions (EMPTY_TRAVEL)
            10:04 - Truck idles at same dump (IDLING)
            10:07 - Truck departing (WORKING) - continued dump detected
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        dump_region = context.dump_region

        base_params = self.factory.create_base_params(last_rec, curr_rec)

        # Calculate implied dump timing
        # Dump started when truck became idle at dump area
        dump_start = timestamp_to_utc_zero(last_rec.current_process_date)
        # Dump ended when truck is moving
        dump_end = timestamp_to_utc_zero(curr_rec.timestamp)

        # Handle based on current segment
        if last_rec.current_segment == CycleSegment.LOAD_TRAVEL.value:
            # Calculate load travel duration if possible
            # From when truck left loader to when it arrived at dump
            load_travel_seconds = None
            if last_rec.load_end_utc:
                load_travel_seconds = self.calculator.calculate_seconds(
                    last_rec.load_end_utc,
                    last_rec.current_process_date
                )
            else:
                logger.warning(
                    f"Cannot calculate load_travel_seconds for cycle "
                    f"{last_rec.cycle_number}: load_end_utc is NULL"
                )

            base_params.update({
                "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                "idle_in_dump_region_guid": None,
                "dump_region_guid": dump_region.region_guid,
                "dump_start_utc": dump_start,
                "dump_end_utc": dump_end,
                "dump_seconds": idle_duration,
                "load_travel_seconds": load_travel_seconds,
            })

        elif last_rec.current_segment == CycleSegment.EMPTY_TRAVEL.value:
            # Truck in EMPTY_TRAVEL returned to same dump for more dumping
            # This handles interrupted or continued dumping scenarios

            # Check if it's the same dump region
            if (
                last_rec.dump_region_guid
                and last_rec.dump_region_guid == dump_region.region_guid
            ):
                # Same dump region - continue dumping
                # Add idle duration to existing dump time
                total_dump_seconds = (
                    (last_rec.dump_seconds or 0) + idle_duration
                )

                base_params.update({
                    "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                    "dump_end_utc": dump_end,
                    "dump_seconds": total_dump_seconds,
                })

                logger.debug(
                    f"Continued dumping at same region for cycle "
                    f"{last_rec.cycle_number}: Additional "
                    f"{idle_duration:.1f}s, total dump time "
                    f"{total_dump_seconds:.1f}s"
                )
            else:
                # Different dump region - this is unusual
                logger.warning(
                    f"Truck in EMPTY_TRAVEL moved to different dump region "
                    f"(was: {last_rec.dump_region_guid}, "
                    f"now: {dump_region.region_guid}) for cycle "
                    f"{last_rec.cycle_number}. Treating as new dump."
                )

                base_params.update({
                    "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                    "idle_in_dump_region_guid": None,
                    "dump_region_guid": dump_region.region_guid,
                    "dump_start_utc": (
                        last_rec.dump_start_utc if last_rec.dump_region_guid
                        else dump_start
                    ),
                    "dump_end_utc": dump_end,
                    "dump_seconds": (
                        (
                            (last_rec.dump_seconds or 0) + idle_duration
                        ) if last_rec.dump_region_guid
                        else idle_duration
                    ),
                })

        elif last_rec.current_segment is None:
            # Initial record case - transition to EMPTY_TRAVEL
            # But cycle will likely be INVALID due to missing segments
            base_params.update({
                "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                "dump_region_guid": dump_region.region_guid,
                "dump_start_utc": dump_start,
                "dump_end_utc": dump_end,
                "dump_seconds": idle_duration,
            })

            logger.warning(
                f"Implied DUMP_TIME from initial record (no segment) "
                f"for cycle {last_rec.cycle_number}. Cycle will likely "
                f"be INVALID due to missing LOAD segments."
            )

        else:
            # Other segments - still transition but with warning
            base_params.update({
                "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                "dump_region_guid": dump_region.region_guid,
                "dump_start_utc": dump_start,
                "dump_end_utc": dump_end,
                "dump_seconds": idle_duration,
            })

            logger.warning(
                f"Implied DUMP_TIME from unexpected segment "
                f"{last_rec.current_segment} for cycle "
                f"{last_rec.cycle_number}. Transitioning to EMPTY_TRAVEL."
            )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _complete_dump_time_to_empty_travel(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, None]:
        """Complete DUMP_TIME and transition to EMPTY_TRAVEL.

        Calculates dump duration and transitions the cycle to EMPTY_TRAVEL
        segment when the truck starts moving after dumping. This represents
        the normal completion of dumping operations.

        The method:
        1. Calculates dump duration from dump_start_utc to current time
        2. Sets dump_end_utc to current timestamp
        3. Transitions to EMPTY_TRAVEL segment
        4. Preserves all dumping metrics for reporting

        Args:
            context: Cycle comparison context containing:
                    - last_record: In DUMP_TIME with dump_start_utc
                    - current_record: Shows truck is now WORKING

        Returns:
            Tuple of (updated_record, None)
                - Updated record with completed dump metrics
                - None (never creates new cycles)

        Business Logic:
            - Dump duration includes entire idle period at dump
            - Empty travel begins immediately when truck moves
            - All dump metrics preserved for cycle completion

        Example:
            10:00 - Started dumping (dump_start_utc)
            10:05 - Truck starts moving (current)
            Result: dump_seconds = 300s (5 min)
                    Segment changes to EMPTY_TRAVEL
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Calculate dump duration
        dump_end_time = curr_rec.timestamp
        dump_seconds = None

        if last_rec.dump_start_utc is not None:
            dump_seconds = self.calculator.calculate_seconds(
                last_rec.dump_start_utc,
                dump_end_time
            )

        # Create base parameters
        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        # Update for EMPTY_TRAVEL transition
        base_params.update({
            "current_segment": CycleSegment.EMPTY_TRAVEL.value,
            "dump_end_utc": dump_end_time,
            "dump_seconds": dump_seconds,
        })

        logger.info(
            f"Completing DUMP_TIME for cycle {last_rec.cycle_number}: "
            f"Dump duration {dump_seconds:.1f}s, "
            f"transitioning to EMPTY_TRAVEL"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None


class TravelingAreaHandler(CycleStateHandler):
    """Handler for traveling area cycle logic with outlier detection.

    Manages cycle state transitions when a haul truck is traveling between
    loading and dumping areas. This includes monitoring for abnormal behavior
    such as extended idle periods during travel segments that indicate
    operational issues.

    The handler's primary responsibility is detecting outlier behavior when
    trucks idle too long in areas that should be transit zones. This helps
    identify breakdowns, traffic issues, or unauthorized stops.

    Key Features:
        - Monitors idle duration in travel segments
        - Marks cycles as outliers when thresholds exceeded
        - Handles outlier recovery by closing cycles and creating new ones
        - Tracks outlier position and duration for analysis
        - Accumulates outlier time for multiple idle events

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Business Rules:
        - Idle > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT marks as outlier
        - Outlier cycles closed when truck resumes movement
        - New cycle created after outlier closure for continuity
        - Outlier position captured at first detection

    Example:
        >>> handler = TravelingAreaHandler()
        >>> updated, new = handler.handle(context)
        >>> if updated and updated.is_outlier:
        ...     print(f"Outlier detected at ({updated.outlier_position_latitude}, "
        ...           f"{updated.outlier_position_longitude})")

    Note:
        Unlike other handlers, TravelingAreaHandler can create new cycles
        when recovering from outlier status to maintain operational continuity.
    """

    def __init__(self: Self) -> None:
        """Initialize the traveling area handler.

        Sets up the factory and calculator instances used throughout
        the handler's operation.
        """
        self.factory = CycleRecordFactory()
        self.calculator = DurationCalculator()

    def handle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle cycle logic in traveling area.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. Primary
        focus is on detecting and handling outlier behavior during travel.

        The method routes to specific handlers based on patterns:
        - Continuous idling: Check for outlier threshold
        - State transitions: Handle movement changes and outlier recovery

        Args:
            context: Cycle comparison context with current/previous records

        Returns:
            Tuple of (updated_record, new_cycle_record) where either or
            both can be None. New cycles are created only when closing
            outlier cycles to maintain tracking continuity.

        Business Impact:
            - Identifies operational disruptions in travel segments
            - Enables tracking of breakdown locations
            - Maintains cycle continuity despite disruptions

        Note:
            Unlike loading/dumping areas, traveling area can create new
            cycles when an outlier cycle needs to be closed. This ensures
            continuous tracking even after operational issues.
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        if self._is_continuous_idling(curr_rec, last_rec):
            return self._handle_continuous_idling(context)
        elif self._is_state_transition(curr_rec, last_rec):
            return self._handle_state_transition(context)

        return None, None

    def _is_continuous_idling(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if truck is continuously idling.

        Determines if the truck is continuously idling in the travel area
        based on both current and previous work states.

        Args:
            curr_rec: Current realtime record
            last_rec: Previous cycle record

        Returns:
            bool: True if continuously idling, False otherwise
        """
        return (
            last_rec.current_work_state_id == WorkState.IDLING
            and curr_rec.work_state_id == WorkState.IDLING
        )

    def _is_state_transition(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if there's a state transition.

        Detects when the truck's work state has changed between
        the previous and current records.

        Args:
            curr_rec: Current realtime record
            last_rec: Previous cycle record

        Returns:
            bool: True if work state has changed, False otherwise
        """
        return curr_rec.work_state_id != last_rec.current_work_state_id

    def _handle_continuous_idling(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle truck continuously idling in travel area.

        Processes scenarios where a truck is continuously stationary in
        the travel area (between loading and dumping zones). Extended idle
        in travel areas indicates operational issues that need tracking.

        The method calculates total idle duration and marks the cycle as
        outlier if the threshold is exceeded. This helps identify:
        - Mechanical breakdowns
        - Traffic congestion
        - Unauthorized stops
        - Driver issues

        Args:
            context: Cycle comparison context containing:
                    - current_record: Shows truck still IDLING
                    - last_record: Shows truck was IDLING

        Returns:
            Tuple of (updated_record, None) or (None, None)
                - Updated record marked as outlier if threshold exceeded
                - (None, None) if still within acceptable idle time

        Business Logic:
            - Idle duration = current timestamp - last timestamp
            - Threshold: IDLE_THRESHOLD_IN_TRAVEL_SEGMENT (e.g., 30 min)
            - Outlier position captured from current GPS coordinates

        Note:
            Uses IDLE_THRESHOLD_IN_TRAVEL_SEGMENT to determine when to
            mark as outlier. Sets outlier position from current record
            to help maintenance teams locate stopped vehicles.
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Calculate idle duration
        idle_duration = self.calculator.calculate_idle_duration(
            previous_process_date=last_rec.current_process_date,
            current_process_date=curr_rec.timestamp
        )

        if idle_duration > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT:
            return self._mark_as_outlier(context, idle_duration)

        # Not idle long enough, no action needed
        # Return empty dict case as None
        return None, None

    def _mark_as_outlier(
        self: Self,
        context: CycleComparisonContext,
        outlier_duration: float
    ) -> Tuple[CycleRecord, None]:
        """Mark the cycle as outlier due to extended idle time.

        Updates the cycle record to mark it as outlier and captures the
        position where the outlier behavior occurred. If the cycle is
        already marked as outlier, accumulates the additional idle time
        to the existing outlier_seconds.

        Args:
            context: Cycle comparison context
            outlier_duration: Total seconds for truck idling in this event

        Returns:
            Tuple of (updated_record, None)

        Business Logic:
            - First outlier event: Sets is_outlier=True, captures position
            - Subsequent events: Accumulates idle time to outlier_seconds
            - Position is only captured on first outlier detection

        Note:
            Outlier tracking helps identify operational issues like
            breakdowns, traffic congestion, or unauthorized stops.
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Create base parameters
        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        # Update for outlier marking
        if last_rec.is_outlier:
            base_params.update({
                "is_outlier": last_rec.is_outlier,
                "outlier_seconds": last_rec.outlier_seconds + outlier_duration,
            })
        else:
            base_params.update({
                "is_outlier": True,
                "outlier_position_latitude": curr_rec.latitude,
                "outlier_position_longitude": curr_rec.longitude,
                "outlier_seconds": outlier_duration,
            })

        logger.warning(
            f"Marking cycle {last_rec.cycle_number} as OUTLIER: "
            f"Idle for too long in {last_rec.current_segment} segment "
            f"at position ({curr_rec.latitude}, {curr_rec.longitude})"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _handle_state_transition(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle work state transitions in traveling area.

        Processes transitions between WORKING and IDLING states:

        1. WORKING -> IDLING: Truck stops moving, update work state
        2. IDLING -> WORKING: Truck starts moving
           - If is_outlier: Close cycle as OUTLIER and create new cycle
           - If idle > threshold: Mark as outlier
           - Otherwise: Just update work state

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record) or (None, None)

        Note:
            This handler can create new cycles when recovering from
            outlier status, ensuring continuous tracking despite
            operational disruptions.
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        # WORKING -> IDLING transition (truck stops moving)
        if (
            last_rec.current_work_state_id == WorkState.WORKING
            and curr_rec.work_state_id == WorkState.IDLING
        ):

            # Just update the work state
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )

            logger.debug(
                f"State transition WORKING->IDLING in travel area "
                f"for cycle {last_rec.cycle_number}"
            )

            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

        # IDLING -> WORKING transition (truck starts moving)
        elif (
            last_rec.current_work_state_id == WorkState.IDLING
            and curr_rec.work_state_id == WorkState.WORKING
        ):

            # Check if this is an outlier cycle
            if last_rec.is_outlier:
                return self._close_outlier_cycle(context)
            else:
                # Calculate idle duration
                idle_duration = self.calculator.calculate_idle_duration(
                    previous_process_date=last_rec.current_process_date,
                    current_process_date=curr_rec.timestamp
                )
                if idle_duration > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT:
                    return self._mark_as_outlier(context, idle_duration)
                else:
                    # Just update the work state
                    base_params = self.factory.create_base_params(
                        last_rec, curr_rec
                    )

                logger.debug(
                    f"State transition IDLING->WORKING in travel area "
                    f"for cycle {last_rec.cycle_number}"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

        # No transition detected
        return None, None

    def _close_outlier_cycle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, CycleRecord]:
        """Close outlier cycle and create a new cycle.

        Closes the current cycle with OUTLIER status and creates a new
        cycle for the truck to continue operations. This ensures continuous
        tracking even after operational disruptions.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record)

        Business Logic:
            - Closes current cycle with OUTLIER status
            - Creates new cycle using factory's outlier recovery params
            - New cycle starts fresh without carrying outlier state

        Note:
            The new cycle allows the truck to resume normal operations
            after resolving whatever issue caused the extended idle.
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Close the outlier cycle
        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        # Update status to OUTLIER
        base_params.update({
            "cycle_status": "OUTLIER",
        })

        logger.info(
            f"Closing OUTLIER cycle {last_rec.cycle_number}: "
            f"Truck resumed movement after extended idle"
        )

        updated_rec = CycleRecord(**base_params)

        # Create new cycle after outlier
        new_params = self.factory.create_outlier_recovery_params(
            curr_rec, last_rec
        )
        new_cycle_rec = CycleRecord(**new_params)

        return updated_rec, new_cycle_rec


class AllAssetsInSameDumpAreaHandler(CycleStateHandler):
    """Handler for managing cycles when haul truck and loader are in same dump area.

    This handler manages the special operational scenario where both a haul truck
    and its associated loader are detected within the same dump area. This situation
    requires specific cycle management to track when assets converge in dump regions
    and to properly close cycles when they separate.

    The handler tracks the `all_assets_in_same_dump_area` flag to monitor when
    both truck and loader enter/exit the same dump region together. This information
    is critical for understanding operational patterns and potential inefficiencies.

    Key Features:
        - Detects when truck and loader enter same dump area
        - Tracks duration of co-location in dump area
        - Closes cycles when assets separate after being together
        - Creates new cycles for continued tracking after separation

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Business Rules:
        - Flag set to True when both assets detected in same dump area
        - Flag remains True while both assets stay in same area
        - Cycle closed when assets separate (flag True -> context False)
        - New cycle created after separation for continuity

    State Transitions:
        1. Not together -> Together: Update flag to True
        2. Together -> Still together: No action needed
        3. Together -> Separated: Close cycle and create new one

    Example:
        >>> handler = AllAssetsInSameDumpAreaHandler()
        >>> updated, new = handler.handle(context)
        >>> if updated and updated.all_assets_in_same_dump_area:
        ...     print("Assets are now in same dump area")
        >>> elif updated and new:
        ...     print("Assets separated - cycle closed and new one created")

    Note:
        This handler is typically called when asset_position indicates a dump
        area and co-location checking is relevant. The handler helps identify
        operational patterns where loaders may be repositioned to dump areas
        or where standard operational flow is disrupted.
    """

    def __init__(self: Self) -> None:
        """Initialize the same dump area handler.

        Sets up the factory and calculator instances used throughout
        the handler's operation.
        """
        self.factory = CycleRecordFactory()
        self.calculator = DurationCalculator()

    def handle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle cycle logic when assets may be in same dump area.

        Processes the cycle comparison context to determine if the truck
        and loader co-location status has changed and takes appropriate
        action based on the transition.

        The method implements three distinct scenarios:
        1. Assets newly together: Set flag to True
        2. Assets remain together: No action needed
        3. Assets newly separated: Close cycle and create new one

        Args:
            context: Cycle comparison context containing:
                - last_record: Previous cycle state
                - current_record: Current asset state
                - assets_in_same_location: Current co-location status

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_record, None) when setting flag to True
                - (None, None) when no change needed
                - (closed_cycle, new_cycle) when assets separate

        Business Logic:
            The handler ensures proper cycle management when operational
            patterns deviate from normal flow, such as when loaders are
            moved to dump areas for maintenance or special operations.
        """
        last_rec = context.last_record

        # Scenario 1: Assets newly together (False -> True)
        if (
            not last_rec.all_assets_in_same_dump_area
            and context.assets_in_same_location
        ):
            return self._handle_assets_newly_together(context)

        # Scenario 2: Assets remain together (True -> True)
        elif (
            last_rec.all_assets_in_same_dump_area
            and context.assets_in_same_location
        ):
            # No action needed - assets still in same location
            logger.debug(
                f"Assets still in same dump area for cycle {last_rec.cycle_number}"
            )
            return None, None

        # Scenario 3: Assets newly separated (True -> False)
        elif (
            last_rec.all_assets_in_same_dump_area
            and not context.assets_in_same_location
        ):
            return self._handle_assets_separated(context)

        # Scenario 4: Assets remain separated (False -> False)
        else:
            # No action needed - normal operation
            return None, None

    def _handle_assets_newly_together(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, None]:
        """Handle when truck and loader newly enter same dump area.

        Updates the cycle record to set the all_assets_in_same_dump_area
        flag to True, indicating that both the truck and its associated
        loader are now in the same dump region.

        This typically occurs in special operational scenarios such as:
        - Loader relocated to dump area for maintenance
        - Special loading operations at dump sites
        - Emergency operational adjustments

        Args:
            context: Cycle comparison context with current states

        Returns:
            Tuple[CycleRecord, None]: Updated record with flag set to True,
                                     no new cycle needed

        Business Impact:
            - Tracks unusual operational patterns
            - Helps identify potential inefficiencies
            - Provides data for operational optimization
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Create base parameters
        base_params = self.factory.create_base_params(last_rec, curr_rec)

        # Update the flag
        base_params.update({
            "all_assets_in_same_dump_area": True
        })

        logger.debug(
            f"Truck and loader entered same dump area for cycle "
            f"{last_rec.cycle_number}"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _handle_assets_separated(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, CycleRecord]:
        """Handle when truck and loader separate after being in same dump area.

        Closes the current cycle and creates a new one when the truck and
        loader are no longer in the same dump area after previously being
        together. This represents the end of an unusual operational pattern
        and return to normal flow.

        The method:
        1. Closes current cycle with appropriate status
        2. Sets cycle_end_utc to current timestamp
        3. Creates new cycle using outlier recovery parameters

        Args:
            context: Cycle comparison context showing separation

        Returns:
            Tuple[CycleRecord, CycleRecord]:
                - Updated record: Current cycle closed
                - New record: Fresh cycle for continued tracking

        Business Logic:
            - Cycle closure indicates end of abnormal pattern
            - New cycle ensures continuous operational tracking
            - Uses outlier recovery params as operational flow is unclear

        Note:
            The new cycle is created with minimal information as the truck's
            position in the normal cycle flow is uncertain after this unusual
            operational pattern.
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Close current cycle
        base_params = self.factory.create_base_params(last_rec, curr_rec)

        # Determine cycle status based on current state
        # Since this is an unusual pattern, mark as OUTLIER
        cycle_status = CycleStatus.OUTLIER.value

        base_params.update({
            "cycle_status": cycle_status,
            "cycle_end_utc": curr_rec.timestamp,
            "all_assets_in_same_dump_area": True
        })

        logger.debug(
            f"Closing cycle {last_rec.cycle_number}: "
            f"Truck and loader separated after being in same dump area"
        )

        updated_rec = CycleRecord(**base_params)

        # Create new cycle for continued tracking
        new_params = self.factory.create_outlier_recovery_params(
            curr_rec, last_rec
        )
        new_cycle_rec = CycleRecord(**new_params)

        logger.info(
            f"Created new cycle {new_cycle_rec.cycle_number} after "
            f"truck-loader separation"
        )

        return updated_rec, new_cycle_rec


class CycleSegmentDecisionEngine:
    """Main engine for cycle segment decisions.

    Central coordinator that routes cycle comparison contexts to the
    appropriate area-specific handlers based on the asset's current
    position. This implements a strategy pattern where different
    handlers are selected dynamically.

    Attributes:
        handlers: Dictionary mapping area names to handler instances

    Example:
        >>> engine = CycleSegmentDecisionEngine()
        >>> updated, new = engine.process_context(context)
    """

    def __init__(self: Self) -> None:
        """Initialize the decision engine with area handlers.

        Sets up the mapping between area identifiers and their
        corresponding handler instances.
        """
        self.handlers = {
            "loading_area": LoadingAreaHandler(),
            "dumping_area": DumpingAreaHandler(),
            "traveling_area": TravelingAreaHandler(),
            "same_dump_area": AllAssetsInSameDumpAreaHandler(),
        }

    def process_context(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Process the context and return updated/new records.

        Routes the context to the appropriate handler based on the
        asset_position field in the context. If no handler is found
        for the position, logs a warning and returns no updates.

        Special handling for same_dump_area checks: This handler is
        called when assets_in_same_location is not None, regardless
        of the specific asset_position value.

        Args:
            context: Cycle comparison context containing asset position

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                Results from the selected handler, or (None, None) if
                no handler found

        Warning:
            Logs a warning if asset_position doesn't match any handler
        """
        # Check if we need to handle same dump area logic
        if (
            context.assets_in_same_location
            or context.last_record.all_assets_in_same_dump_area
        ):
            handler = self.handlers.get("same_dump_area")
            if handler:
                return handler.handle(context)

        # Otherwise use position-based routing
        handler = self.handlers.get(context.asset_position)
        if handler:
            return handler.handle(context)

        logger.warning(
            f"No handler found for position: {context.asset_position}"
        )
        return None, None


def cycle_records_comparison(
    context: CycleComparisonContext
) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
    """Main entry point for cycle record comparison.

    This function serves as the public API for the module, processing
    cycle comparison contexts to determine if cycle records need to be
    updated or if new cycles should be created.

    The function routes the context to the appropriate handler based on
    the asset's current position (loading_area, dumping_area, or
    traveling_area) and returns the results.

    Args:
        context: The cycle comparison context containing:
            - current_record: Current state of the asset
            - last_record: Previous state of the asset
            - asset_position: Current area (loading/dumping/traveling)
            - loader_asset: Information about nearby loader (if any)
            - loader_distance: Distance to loader
            - Other context-specific data

    Returns:
        Tuple[Optional[CycleRecord], Optional[CycleRecord]]: A tuple where:
            - First element: Updated version of the last record (if any)
            - Second element: New cycle record to create (if any)
            Either or both elements can be None if no updates are needed

    Example:
        >>> from models.dto.method_dto import CycleComparisonContext
        >>>
        >>> context = CycleComparisonContext(
        ...     current_record=current_state,
        ...     last_record=previous_state,
        ...     asset_position="loading_area",
        ...     loader_asset=loader_info,
        ...     loader_distance=15.5
        ... )
        >>>
        >>> updated_record, new_cycle = cycle_records_comparison(context)
        >>>
        >>> if updated_record:
        ...     # Save the updated record to database
        ...     save_record(updated_record)
        >>>
        >>> if new_cycle:
        ...     # Create new cycle in database
        ...     create_cycle(new_cycle)

    Note:
        This is the only public function in this module. All other
        functions and classes are implementation details that should
        not be used directly by external code.
    """
    engine = CycleSegmentDecisionEngine()
    return engine.process_context(context)
