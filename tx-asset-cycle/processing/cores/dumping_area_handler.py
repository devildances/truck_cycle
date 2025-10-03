import logging
from typing import Optional, Self, Tuple

from config.static_config import (IDLE_THRESHOLD_FOR_START_DUMP,
                                  IDLE_THRESHOLD_IN_GEOFENCE_AREA)
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import DurationCalculator, timestamp_to_utc_zero

from .identifiers import (CycleRecordFactory, CycleSegment, CycleStateHandler,
                          CycleStatus, WorkState)

logger = logging.getLogger(__name__)


class DumpingAreaHandler(CycleStateHandler):
    """Handler for dumping area cycle logic with comprehensive dump management.

    Manages cycle state transitions when a haul truck is in the dumping area,
    including dump initiation, dump completion, implied dumping scenarios,
    outlier detection, and continued dumping at the same region.

    The handler supports multiple operational scenarios:
    1. Normal dumping: Explicit dump detection through continuous idling
    2. Implied dumping: Dump operations that occurred between GPS updates
    3. Continued dumping: Trucks repositioning within the same dump region
    4. Outlier detection: Extended idle periods exceeding thresholds
    5. Outlier recovery: Creating new cycles after outlier resolution

    Key Features:
        - Detects dumping start based on idle duration thresholds
        - Transitions from LOAD_TRAVEL to DUMP_TIME when threshold exceeded
        - Handles implied dumping for low-frequency GPS updates
        - Manages dump completion and transition to EMPTY_TRAVEL
        - Supports continued dumping at same region (repositioning)
        - Records dump region information for tracking
        - Detects outlier behavior for extended idle periods
        - Creates new cycles when recovering from outlier status

    Attributes:
        factory (CycleRecordFactory): Instance for creating cycle records
        calculator (DurationCalculator): Instance for time calculations

    Business Rules:
        - Truck must idle > IDLE_THRESHOLD_FOR_START_DUMP to start dumping
        - Dumping completes when truck transitions from IDLING to WORKING
        - Extended idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA marks as outlier
        - Implied dumping handles cases where GPS missed dumping state
        - Same dump region validated for continued dumping scenarios

    Example:
        >>> handler = DumpingAreaHandler()
        >>> updated, new = handler.handle(context)
        >>> if updated and updated.current_segment == CycleSegment.DUMP_TIME:
        ...     print(f"Started dumping at region {updated.dump_region_guid}")
        >>> elif updated and updated.current_segment == CycleSegment.EMPTY_TRAVEL:
        ...     print(f"Completed dumping, duration: {updated.dump_seconds}s")

    Note:
        Unlike LoadingAreaHandler, DumpingAreaHandler typically doesn't create
        new cycles except when recovering from outlier status. It primarily
        updates existing cycles with dumping information. The handler now
        includes sophisticated logic for handling continued dumping operations
        where trucks may reposition within the same dump region.
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
        """Handle cycle logic in dumping area with multiple state patterns.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. Routes
        to specific handlers for different operational patterns.

        The method identifies two main patterns:
        1. Continuous idling: Both states IDLING (check thresholds)
        2. State transition: Different states (handle various transitions)

        Args:
            context (CycleComparisonContext): Contains current/previous records,
                dump region information, and spatial context

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (None, None) if no action needed
                - (updated_record, None) for most dump operations
                - (updated_record, new_cycle) only for outlier recovery

        Note:
            The dumping area handler focuses on updating existing cycles
            rather than creating new ones, except in outlier recovery cases.
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

        Priority order:
        1. Already marked as outlier: Skip processing
        2. Exceeds geofence threshold: Mark as outlier
        3. Already in DUMP_TIME: Continue dumping (no action)
        4. In LOAD_TRAVEL: Transition to DUMP_TIME
        5. Other segments: Transition to DUMP_TIME (with warning)

        Args:
            context (CycleComparisonContext): Contains segment and region info
            idle_duration (float): Total seconds truck has been idling

        Returns:
            Tuple[Optional[CycleRecord], None]:
                - (None, None) if already outlier or in DUMP_TIME
                - (updated_record, None) for segment transitions or outlier marking

        Business Logic:
            Outlier detection (idle > IDLE_THRESHOLD_IN_GEOFENCE_AREA):
            - Sets is_outlier = True
            - Records outlier_type_id = 2 (dump area outlier)
            - Captures GPS position and dump region

            Normal transitions:
            - LOAD_TRAVEL → DUMP_TIME: Expected flow, calculates load_travel_seconds
            - Other → DUMP_TIME: Unexpected but handled, logs warning
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        segment = last_rec.current_segment
        dump_region = context.dump_region

        if last_rec.is_outlier:
            return None, None
        elif idle_duration > IDLE_THRESHOLD_IN_GEOFENCE_AREA:
            base_params = self.factory.create_base_params(
                last_rec, curr_rec
            )
            base_params.update({
                "is_outlier": True,
                "outlier_type_id": 2,
                "outlier_date_utc": last_rec.current_process_date,
                "outlier_dump_region_guid": dump_region.region_guid,
                "outlier_position_latitude": last_rec.current_asset_latitude,
                "outlier_position_longitude": last_rec.current_asset_longitude,
                "outlier_seconds": idle_duration
            })
            updated_rec = CycleRecord(**base_params)
            return updated_rec, None
        else:
            if segment == CycleSegment.DUMP_TIME.value:
                # Already in DUMP_TIME, no action needed
                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
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

        Updates the cycle record when a truck that was traveling with load
        has been idle long enough to start dumping. This is the normal
        progression in the cycle flow.

        The method:
        1. Validates dump region information is available
        2. Calculates load travel duration from load_end_utc to arrival
        3. Sets dump_start_utc to when idling began (not current time)
        4. Updates segment to DUMP_TIME with dump region info

        Args:
            context (CycleComparisonContext): Contains dump region and timing info

        Returns:
            Tuple[CycleRecord, None]:
                - Updated record with DUMP_TIME segment and calculations
                - None (never creates new cycles)

        Raises:
            Logs error and returns (None, None) if dump_region is missing

        Business Logic:
            - Load travel time = arrival at dump - load completion
            - Dump starts when truck became idle (previous timestamp)
            - Dump region tracked for operational reporting

        Example Timeline:
            09:45 - Load completed (load_end_utc)
            09:55 - Arrived at dump, started idling (last_rec.current_process_date)
            09:58 - Still idling, threshold exceeded (current)
            Result: load_travel_seconds = 600s (10 min)
                   dump_start_utc = 09:55 (when idling began)
        """
        last_rec = context.last_record
        curr_rec = context.current_record
        dump_region = context.dump_region

        if not dump_region:
            logger.error(
                f"[CORE ERROR] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Cannot start DUMP_TIME without dump region information "
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

        This unusual scenario might occur due to:
        - GPS data gaps causing missed state transitions
        - Operational irregularities
        - Initial records starting at dump area
        - System restarts during operations

        Args:
            context (CycleComparisonContext): Contains dump region information

        Returns:
            Tuple[CycleRecord, None]:
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
                f"[CORE ERROR] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Cannot start DUMP_TIME without dump region information "
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
            f"[CORE WARNING] {curr_rec.asset_guid} "
            f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
            "Starting DUMP_TIME from unexpected segment "
            f"{last_rec.current_segment} for cycle {last_rec.cycle_number}"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _handle_state_transition(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle work state transitions in dumping area with complex logic.

        Processes transitions between WORKING and IDLING states when the
        truck is in the dumping area. Implements sophisticated handling for
        normal dump operations, implied dumping, continued dumping, and
        outlier recovery scenarios.

        State Transitions:
            1. WORKING -> IDLING: Truck stops, updates state and sets
               idle_in_dump_region_guid
            2. IDLING -> WORKING: Complex logic based on conditions:
               - If outlier or idle > geofence threshold: Outlier recovery
               - If idle > dump threshold: Implied dumping occurred
               - If in DUMP_TIME: Normal dump completion
               - Otherwise: Simple state update

        Special Features:
            - Handles continued dumping at same region (EMPTY_TRAVEL case)
            - Tracks idle_in_dump_region_guid for region validation
            - Manages outlier recovery with new cycle creation

        Args:
            context (CycleComparisonContext): Contains transition information,
                dump region details, and timing context

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_record, None) for normal transitions
                - (updated_record, new_cycle) for outlier recovery
                - (None, None) if no action needed

        Business Logic:
            The method now includes sophisticated handling for:
            - Outlier detection and recovery
            - Implied dumping scenarios
            - Continued dumping at same region
            - Normal dump completions
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
                f"[CORE DEBUG] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "State transition WORKING->IDLING in dump area "
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

                base_params.update({
                    "idle_in_dump_region_guid": None,
                })

                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "State transition IDLING->WORKING in dump area "
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
            context (CycleComparisonContext): Contains cycle and dump region info
            idle_duration (float): Total seconds truck was idle at dump area

        Returns:
            Tuple[CycleRecord, CycleRecord]:
                - Updated record: Previous cycle closed with OUTLIER status
                - New record: Fresh cycle started with no segment

        Business Logic:
            - Closes current cycle with OUTLIER status
            - Accumulates outlier_seconds if already marked as outlier
            - Sets outlier_type_id = 2 for dump area outlier
            - Creates new cycle starting from no segment
            - Preserves cycle continuity despite disruptions

        Example Timeline:
            10:00 - Truck arrives at dump area
            10:05 - Truck starts idling (equipment issue)
            11:35 - Still idling (90 min > geofence threshold)
            11:40 - Truck resumes operation
            Result: Previous cycle closed as OUTLIER
                   New minimal cycle created for tracking
        """
        curr_rec = context.current_record
        last_rec = context.last_record
        dump_region = context.dump_region

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
                "outlier_type_id": 2,
                "outlier_date_utc": last_rec.current_process_date,
                "outlier_dump_region_guid": dump_region.region_guid,
                "outlier_position_latitude": last_rec.current_asset_latitude,
                "outlier_position_longitude": last_rec.current_asset_longitude,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
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
        """Handle implied dumping where truck dumped between GPS updates.

        Processes scenarios where a truck was idle in the dump area long
        enough to complete dumping but the next GPS record shows it's already
        moving. This handles real-world GPS tracking limitations and supports
        continued dumping operations at the same region.

        The method implements segment-specific logic:
        1. LOAD_TRAVEL: Normal flow, calculates load_travel_seconds and
           dump_seconds from idle period
        2. EMPTY_TRAVEL: Continued dumping at same region (truck repositioned
           and dumped more), validates same dump region
        3. None (initial): Transitions but likely creates INVALID cycle
        4. Other segments: Logs warning but still transitions

        Special handling for EMPTY_TRAVEL (continued dumping):
        - Validates truck is at same dump region as before
        - Accumulates dump time to existing dump_seconds
        - Handles trucks that need multiple dump positions
        - Logs warning if different dump region detected

        Args:
            context (CycleComparisonContext): Contains dump region and timing info
            idle_duration (float): Seconds truck was idle (> dump threshold)

        Returns:
            Tuple[CycleRecord, None]:
                - Updated record with EMPTY_TRAVEL segment and dump metrics
                - None (never creates new cycles)

        Business Logic:
            Time Calculations:
            - dump_start_utc: When truck became idle (previous timestamp)
            - dump_end_utc: Current time (truck now moving)
            - dump_seconds: Duration of idle period
            - For EMPTY_TRAVEL: Accumulates to existing dump_seconds

            Continued Dumping Validation:
            - Same region: Add idle_duration to existing dump_seconds
            - Different region: Log warning, treat as new dump

        Example Timelines:
            Case 1 - Normal dump (LOAD_TRAVEL):
            10:00 - Truck arrives at dump
            10:00 - Truck idles
            10:05 - Truck moving - dump complete
            Result: dump_seconds = 300s, transitions to EMPTY_TRAVEL

            Case 2 - Continued dump (EMPTY_TRAVEL):
            10:00 - First dump completed
            10:03 - Truck repositions within same dump area
            10:04 - Truck idles again
            10:07 - Truck departing
            Result: dump_seconds accumulates (original + 180s)
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
                    f"[CORE WARNING] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Cannot calculate load_travel_seconds for cycle "
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

            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #729 | ADD DUMP SECONDS VALUE {idle_duration}s | PREVIOUS SEGMENT IS LOAD_TRAVEL\n")

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
                    "idle_in_dump_region_guid": None,
                })

                with open("/usr/src/app/debug.txt", "a") as f:
                    f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #754 | ADD DUMP SECONDS VALUE {last_rec.dump_seconds}s + {idle_duration}s | PREVIOUS SEGMENT IS EMPTY_TRAVEL\n")

                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Continued dumping at same region for cycle "
                    f"{last_rec.cycle_number}: Additional "
                    f"{idle_duration:.1f}s, total dump time "
                    f"{total_dump_seconds:.1f}s"
                )
            else:
                # Different dump region - this is unusual
                logger.warning(
                    f"[CORE WARNING] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Truck in EMPTY_TRAVEL moved to different dump region "
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

                if last_rec.dump_region_guid:
                    with open("/usr/src/app/debug.txt", "a") as f:
                        f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #784 | ADD DUMP SECONDS VALUE {last_rec.dump_seconds}s + {idle_duration}s | PREVIOUS SEGMENT IS EMPTY_TRAVEL\n")
                else:
                    with open("/usr/src/app/debug.txt", "a") as f:
                        f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #784 | ADD DUMP SECONDS VALUE {idle_duration}s | PREVIOUS SEGMENT IS EMPTY_TRAVEL\n")

        elif last_rec.current_segment is None:
            # Initial record case - transition to EMPTY_TRAVEL
            # But cycle will likely be INVALID due to missing segments
            base_params.update({
                "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                "dump_region_guid": dump_region.region_guid,
                "dump_start_utc": dump_start,
                "dump_end_utc": dump_end,
                "dump_seconds": idle_duration,
                "idle_in_dump_region_guid": None,
            })

            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #807 | ADD DUMP SECONDS VALUE {idle_duration}s | PREVIOUS SEGMENT IS NONE\n")

            logger.warning(
                f"[CORE WARNING] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Implied DUMP_TIME from initial record (no segment) "
                f"for cycle {last_rec.cycle_number}. Cycle will likely "
                "be INVALID due to missing LOAD segments."
            )

        else:
            # Other segments - still transition but with warning
            base_params.update({
                "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                "dump_region_guid": dump_region.region_guid,
                "dump_start_utc": dump_start,
                "dump_end_utc": dump_end,
                "dump_seconds": idle_duration,
                "idle_in_dump_region_guid": None,
            })

            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #826 | ADD DUMP SECONDS VALUE {idle_duration}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

            logger.warning(
                f"[CORE WARNING] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Implied DUMP_TIME from unexpected segment "
                f"{last_rec.current_segment} for cycle "
                f"{last_rec.cycle_number}. Transitioning to EMPTY_TRAVEL."
            )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _complete_dump_time_to_empty_travel(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, None]:
        """Complete DUMP_TIME segment and transition to EMPTY_TRAVEL.

        Calculates dump duration and transitions the cycle to EMPTY_TRAVEL
        segment when the truck starts moving after explicit dumping operations.
        This represents the normal completion of dumping where the truck was
        already in DUMP_TIME segment.

        The method:
        1. Calculates dump duration from dump_start_utc to current time
        2. Sets dump_end_utc to current timestamp
        3. Transitions to EMPTY_TRAVEL segment
        4. Clears idle_in_dump_region_guid
        5. Preserves all dumping metrics for reporting

        Args:
            context (CycleComparisonContext): Contains timing and transition info

        Returns:
            Tuple[CycleRecord, None]:
                - Updated record with completed dump metrics and EMPTY_TRAVEL
                - None (never creates new cycles)

        Business Logic:
            - Dump duration = current time - dump_start_utc
            - Empty travel begins immediately when truck moves
            - All dump metrics preserved for cycle completion
            - Handles missing dump_start_utc gracefully

        Example:
            10:00 - Started dumping (dump_start_utc)
            10:05 - Truck starts moving (current)
            Result: dump_seconds = 300s (5 min)
                   Segment changes to EMPTY_TRAVEL
                   dump_end_utc = 10:05

        Note:
            This is the normal flow for dump completion, contrasting with
            implied dump completion which handles GPS gaps. The truck was
            explicitly tracked in DUMP_TIME before this transition.
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
            "idle_in_dump_region_guid": None,
        })

        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | dumping_area_handler.py #899 | ADD DUMP SECONDS VALUE {dump_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

        logger.info(
            f"Completing DUMP_TIME for cycle {last_rec.cycle_number}: "
            f"Dump duration {dump_seconds:.1f}s, "
            f"transitioning to EMPTY_TRAVEL"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None
