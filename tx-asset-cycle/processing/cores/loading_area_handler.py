import logging
from datetime import timedelta
from typing import Optional, Self, Tuple

from config.static_config import (DISTANCE_MAX_FOR_LOADER_MOVEMENT,
                                  IDLE_THRESHOLD_FOR_START_LOAD,
                                  IDLE_THRESHOLD_IN_GEOFENCE_AREA)
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
