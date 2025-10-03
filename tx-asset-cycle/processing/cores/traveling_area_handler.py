import logging
import json
from typing import Optional, Self, Tuple

from config.static_config import (IDLE_THRESHOLD_IN_TRAVEL_SEGMENT,
                                  IDLE_THRESHOLD_FOR_LOAD_AFTER_IN_TRAVEL_AREA,
                                  IDLE_THRESHOLD_FOR_DUMP_AFTER_IN_TRAVEL_AREA)
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import DurationCalculator, timestamp_to_utc_zero

from .identifiers import (CycleRecordFactory, CycleStateHandler, CycleStatus,
                          WorkState, CycleSegment)

logger = logging.getLogger(__name__)


class TravelingAreaHandler(CycleStateHandler):
    """Handler for traveling area cycle logic with outlier detection.

    Manages cycle state transitions when a haul truck is traveling between
    loading and dumping areas. This handler has expanded functionality beyond
    simple outlier detection to handle cycle completions and dump completions
    that occur while the truck is technically in a travel area.

    The handler now processes three main scenarios:
    1. Outlier detection: Extended idle periods indicating operational issues
    2. Cycle completion: When trucks in EMPTY_TRAVEL complete cycles from
       travel areas
    3. Dump completion: When trucks complete dumping operations while in
       travel areas

    Key Features:
        - Monitors idle duration in travel segments for outlier behavior
        - Handles cycle completion when EMPTY_TRAVEL trucks idle near loaders
        - Processes dump completion for LOAD_TRAVEL/DUMP_TIME segments
        - Creates new cycles after closing completed or outlier cycles
        - Tracks outlier position and duration for operational analysis
        - Manages implied loading and dumping operations

    Attributes:
        factory (CycleRecordFactory): Instance for creating cycle records
        calculator (DurationCalculator): Instance for time calculations

    Business Rules:
        - Idle > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT marks as outlier
        - Idle > IDLE_THRESHOLD_FOR_LOAD_AFTER_IN_TRAVEL_AREA triggers
          cycle completion for EMPTY_TRAVEL segments
        - Idle > IDLE_THRESHOLD_FOR_DUMP_AFTER_IN_TRAVEL_AREA triggers
          dump completion for LOAD_TRAVEL/DUMP_TIME segments
        - New cycles created after cycle closure for continuity

    Example:
        >>> handler = TravelingAreaHandler()
        >>> updated, new = handler.handle(context)
        >>> if updated and updated.cycle_status == CycleStatus.COMPLETE:
        ...     print(f"Cycle {updated.cycle_number} completed from travel area")
        >>> elif updated and updated.is_outlier:
        ...     print(f"Outlier detected at ({updated.outlier_position_latitude}, "
        ...           f"{updated.outlier_position_longitude})")

    Note:
        This handler can create new cycles in multiple scenarios:
        - After closing outlier cycles
        - After completing cycles from EMPTY_TRAVEL
        - The handler uses information from tmp_idle_near_loader for loader
          details when creating new cycles
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
        """Handle cycle logic in traveling area with multiple scenarios.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. The handler
        now manages outlier detection, cycle completions, and dump completions
        all within the travel area context.

        The method routes to specific handlers based on work state patterns:
        - Continuous idling: Check for outlier threshold or completion triggers
        - State transitions: Handle movement changes, outlier recovery, and
          operational completions

        Args:
            context (CycleComparisonContext): Contains current/previous records,
                asset position, and related spatial information

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]: 
                - (updated_record, new_cycle_record) where either or both can
                  be None
                - New cycles are created when closing outlier cycles or
                  completing operational cycles

        Business Impact:
            - Identifies operational disruptions in travel segments
            - Enables completion of cycles without entering load areas
            - Handles dump operations that complete in travel zones
            - Maintains cycle continuity across all scenarios
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

        Determines if the truck has remained stationary between the previous
        and current records.

        Args:
            curr_rec (RealtimeRecord): Current telemetry record
            last_rec (CycleRecord): Previous cycle record

        Returns:
            bool: True if both states show IDLING, False otherwise
        """
        return (
            last_rec.current_work_state_id == WorkState.IDLING
            and curr_rec.work_state_id == WorkState.IDLING
        )

    def _is_state_transition(
        self: Self, curr_rec: RealtimeRecord, last_rec: CycleRecord
    ) -> bool:
        """Check if there's a work state transition.

        Detects when the truck's operational state has changed between
        WORKING and IDLING.

        Args:
            curr_rec (RealtimeRecord): Current telemetry record
            last_rec (CycleRecord): Previous cycle record

        Returns:
            bool: True if work state changed, False if unchanged
        """
        return curr_rec.work_state_id != last_rec.current_work_state_id

    def _handle_continuous_idling(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle truck continuously idling in travel area.

        Processes scenarios where a truck remains stationary in the travel
        area. Extended idle periods may indicate operational issues requiring
        outlier marking.

        The method calculates idle duration and marks the cycle as outlier
        if the threshold is exceeded. This helps identify:
        - Mechanical breakdowns
        - Traffic congestion
        - Unauthorized stops
        - Driver issues

        Args:
            context (CycleComparisonContext): Contains current and previous
                records with truck remaining in IDLING state

        Returns:
            Tuple[Optional[CycleRecord], None]: 
                - (updated_record, None) if outlier threshold exceeded
                - (None, None) if within acceptable idle time

        Business Logic:
            - Idle duration = current timestamp - previous timestamp
            - Threshold: IDLE_THRESHOLD_IN_TRAVEL_SEGMENT (default 600s)
            - Outlier position captured from current GPS coordinates
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

        Updates the cycle record to indicate outlier status and captures
        the position where the abnormal behavior occurred. For already marked
        outliers, accumulates additional idle time to track total disruption.

        Args:
            context (CycleComparisonContext): Contains cycle information
            outlier_duration (float): Idle duration in seconds for this event

        Returns:
            Tuple[CycleRecord, None]: Updated record with outlier status and
                accumulated duration

        Business Logic:
            - First outlier event: Sets is_outlier=True, captures GPS position,
              initializes outlier_seconds
            - Subsequent events: Accumulates time to existing outlier_seconds
            - Position captured only on first detection for root cause analysis
            - outlier_type_id=4 indicates travel area outlier
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
                "outlier_position_latitude": last_rec.current_asset_latitude,
                "outlier_position_longitude": last_rec.current_asset_longitude,
                "outlier_seconds": outlier_duration,
                "outlier_type_id": 4,
                "outlier_date_utc": last_rec.current_process_date,
            })

        logger.warning(
            f"[CORE WARNING] {curr_rec.asset_guid} "
            f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"Marking cycle {last_rec.cycle_number} as OUTLIER: "
            f"Idle for too long in {last_rec.current_segment} segment "
            f"at position ({curr_rec.latitude}, {curr_rec.longitude})"
        )

        updated_rec = CycleRecord(**base_params)
        return updated_rec, None

    def _handle_state_transition(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle work state transitions with expanded logic for completions.

        Processes transitions between WORKING and IDLING states, now including
        logic for cycle completions and dump completions that occur in travel
        areas. This expanded functionality handles trucks that complete
        operations without entering their expected zones.

        State Transitions:
            1. WORKING -> IDLING: Truck stops, only updates work state
            2. IDLING -> WORKING: Complex logic based on cycle state:
               - If outlier: Close cycle and create new
               - If EMPTY_TRAVEL in LOAD/TRAVEL area and idle > threshold:
                 Complete cycle
               - If LOAD_TRAVEL/DUMP_TIME in DUMP area and idle > threshold:
                 Complete dump
               - Otherwise: Check for outlier threshold or update state

        Args:
            context (CycleComparisonContext): Contains transition information
                including current/previous records and area context

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_record, None) for state updates
                - (closed_cycle, new_cycle) for completions
                - (None, None) if no action needed

        Business Logic:
            The method now handles three distinct completion scenarios:
            1. Outlier recovery
            2. Cycle completion from EMPTY_TRAVEL
            3. Dump completion from LOAD_TRAVEL/DUMP_TIME
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
                f"[CORE DEBUG] {curr_rec.asset_guid} "
                f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "State transition WORKING->IDLING in travel area "
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

                if (
                    (last_rec.current_segment == CycleSegment.EMPTY_TRAVEL.value)
                    and (last_rec.current_area in ["LOAD", "TRAVEL"])
                ):
                    return self._handle_cycle_completion(context, idle_duration)
                elif (
                    (
                        last_rec.current_segment in [
                            CycleSegment.LOAD_TRAVEL.value,
                            CycleSegment.DUMP_TIME.value
                        ]
                    )
                    and (last_rec.current_area == "DUMP")
                ):
                    return self._handle_dump_completion(context, idle_duration)
                elif idle_duration > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT:
                    return self._mark_as_outlier(context, idle_duration)
                else:
                    # Just update the work state
                    base_params = self.factory.create_base_params(
                        last_rec, curr_rec
                    )

                logger.debug(
                    f"[CORE DEBUG] {curr_rec.asset_guid} "
                    f"{curr_rec.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "State transition IDLING->WORKING in travel area "
                    f"for cycle {last_rec.cycle_number}"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

        # No transition detected
        return None, None

    def _close_outlier_cycle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, CycleRecord]:
        """Close outlier cycle and create a new cycle for continuity.

        Closes the current cycle with OUTLIER status when the truck resumes
        movement after extended idle period. Creates a new cycle to continue
        tracking operations after the disruption is resolved.

        Args:
            context (CycleComparisonContext): Contains cycle information

        Returns:
            Tuple[CycleRecord, CycleRecord]: 
                - Updated record: Current cycle closed with OUTLIER status
                - New record: Fresh cycle for continued tracking

        Business Logic:
            - Sets cycle_status to OUTLIER
            - Sets cycle_end_utc to current timestamp
            - Creates minimal new cycle using outlier recovery parameters
            - New cycle starts without segment assignment (unknown position)
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Close the outlier cycle
        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        # Update status to OUTLIER
        base_params.update({
            "cycle_status": CycleStatus.OUTLIER.value,
            "cycle_end_utc": curr_rec.timestamp
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

    def _handle_cycle_completion(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[CycleRecord, CycleRecord]:
        """Handle cycle completion from EMPTY_TRAVEL in travel/load areas.

        Processes scenarios where a truck in EMPTY_TRAVEL segment completes
        its cycle while technically in a travel area or has returned to a
        load area. This handles implied loading operations where the truck
        was idle long enough to load but is now moving.

        The method handles two area-specific scenarios:
        1. LOAD area: Creates new cycle with implied loading using stored
           loader information
        2. TRAVEL area: Creates new cycle without segment (position unknown)

        Args:
            context (CycleComparisonContext): Contains cycle and area information
            idle_duration (float): Duration truck was idle in seconds

        Returns:
            Tuple[CycleRecord, CycleRecord]:
                - Updated record: Previous cycle closed (COMPLETE/INVALID)
                - New record: New cycle with appropriate initialization
                  Returns (updated_rec, None) if idle duration doesn't exceed
                  IDLE_THRESHOLD_FOR_LOAD_AFTER_IN_TRAVEL_AREA

        Business Logic:
            - Checks if idle_duration > IDLE_THRESHOLD_FOR_LOAD_AFTER_IN_TRAVEL_AREA
            - If exceeded:
              * Closes current cycle calculating empty_travel_seconds
              * Determines status (COMPLETE if all segments present, else INVALID)
              * Creates new cycle with area-specific initialization
            - LOAD area: Uses tmp_idle_near_loader for loader details, sets
              LOAD_TRAVEL segment with implied loading
            - TRAVEL area: Creates minimal cycle without segment assignment
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        if idle_duration > IDLE_THRESHOLD_FOR_LOAD_AFTER_IN_TRAVEL_AREA:

            # Step 1: Close previous cycle
            empty_travel_seconds = None
            if last_rec.dump_end_utc:
                empty_travel_seconds = self.calculator.calculate_seconds(
                    last_rec.dump_end_utc,
                    last_rec.current_process_date
                )

            # Determine cycle status
            if all([
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
                "cycle_end_utc": last_rec.current_process_date,
                "idle_in_dump_region_guid": None,
                "tmp_idle_near_loader": None,
            })

            updated_rec = CycleRecord(**base_params)

            # Step 2: Create new cycle with implied loading
            new_params = self.factory.create_new_cycle_params(
                curr_rec, last_rec, context
            )

            if last_rec.current_area == "LOAD":
                load_start = timestamp_to_utc_zero(last_rec.current_process_date)
                load_end = timestamp_to_utc_zero(curr_rec.timestamp)
                loader_info = json.loads(last_rec.tmp_idle_near_loader)

                new_params.update({
                    "current_segment": CycleSegment.LOAD_TRAVEL.value,
                    "previous_work_state_id": (
                        last_rec.current_work_state_id
                    ),
                    "load_start_utc": load_start,
                    "load_end_utc": load_end,
                    "load_seconds": idle_duration,
                    "loader_asset_guid": loader_info["loader_asset_guid"],
                    "loader_latitude": loader_info["loader_latitude"],
                    "loader_longitude": loader_info["loader_longitude"],
                    "load_region_guid": loader_info["load_region_guid"],
                    "is_within_load_region": loader_info[
                        "is_within_load_region"
                    ],
                    "asset_load_region_distance": loader_info[
                        "asset_load_region_distance"
                    ],
                    "current_loader_distance": None,
                    "cycle_start_utc": last_rec.current_process_date,
                })

            elif last_rec.current_area == "TRAVEL":
                new_params.update({
                    "current_segment": None,
                    "previous_work_state_id": (
                        last_rec.current_work_state_id
                    ),
                    "current_loader_distance": None,
                    "cycle_start_utc": last_rec.current_process_date,
                })

            new_cycle_rec = CycleRecord(**new_params)

            return updated_rec, new_cycle_rec
        else:
            updated_rec = CycleRecord(**base_params)
            return updated_rec, None

    def _handle_dump_completion(
        self: Self,
        context: CycleComparisonContext,
        idle_duration: float
    ) -> Tuple[CycleRecord, CycleRecord]:
        """Handle dump completion for trucks in LOAD_TRAVEL or DUMP_TIME segments.

        Processes scenarios where a truck completes dumping operations while
        in a travel area designated as DUMP. This handles implied dumping where
        the truck was idle long enough to dump but is now moving.

        The method handles two segment-specific scenarios:
        1. LOAD_TRAVEL: Truck arrived at dump, idled, and is now leaving
        2. DUMP_TIME: Truck continues dumping (accumulates dump time)

        Args:
            context (CycleComparisonContext): Contains cycle and segment information
            idle_duration (float): Duration truck was idle in seconds

        Returns:
            Tuple[CycleRecord, None]: 
                - Updated record with dump completion and EMPTY_TRAVEL segment
                - None (no new cycle created for dump completions)

        Business Logic:
            - Checks if idle_duration > IDLE_THRESHOLD_FOR_DUMP_AFTER_IN_TRAVEL_AREA
            - If exceeded:
              * Sets segment to EMPTY_TRAVEL
              * Sets dump_end_utc to current timestamp
              * For LOAD_TRAVEL: Calculates fresh dump_seconds and load_travel_seconds
              * For DUMP_TIME: Accumulates to existing dump_seconds
            - Uses idle_in_dump_region_guid for dump region tracking
            - Clears idle_in_dump_region_guid after processing
        """
        curr_rec = context.current_record
        last_rec = context.last_record

        base_params = self.factory.create_base_params(
            last_rec, curr_rec
        )

        base_params.update({
            "idle_in_dump_region_guid": None,
        })

        if idle_duration > IDLE_THRESHOLD_FOR_DUMP_AFTER_IN_TRAVEL_AREA:

            dump_start = timestamp_to_utc_zero(last_rec.current_process_date)
            dump_end = timestamp_to_utc_zero(curr_rec.timestamp)
            base_params.update({
                "current_segment": CycleSegment.EMPTY_TRAVEL.value,
                "dump_end_utc": dump_end,
            })

            if last_rec.current_segment == CycleSegment.LOAD_TRAVEL.value:
                dump_seconds = self.calculator.calculate_idle_duration(
                    previous_process_date=dump_start,
                    current_process_date=dump_end
                )
                load_travel_seconds = None
                if last_rec.load_end_utc:
                    load_travel_seconds = self.calculator.calculate_seconds(
                        last_rec.load_end_utc,
                        last_rec.current_process_date
                    )
                base_params.update({
                    "dump_region_guid": last_rec.idle_in_dump_region_guid,
                    "dump_start_utc": dump_start,
                    "dump_seconds": dump_seconds,
                    "load_travel_seconds": load_travel_seconds,
                })
                with open("/usr/src/app/debug.txt", "a") as f:
                    f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | traveling_area_handler.py #537 | ADD DUMP SECONDS VALUE {dump_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")
            elif last_rec.current_segment == CycleSegment.DUMP_TIME.value:
                dump_seconds = self.calculator.calculate_idle_duration(
                    previous_process_date=last_rec.dump_start_utc,
                    current_process_date=dump_end
                )
                if last_rec.dump_seconds:
                    dump_seconds += last_rec.dump_seconds
                base_params.update({
                    "dump_region_guid": last_rec.dump_region_guid,
                    "dump_seconds": dump_seconds,
                })

                with open("/usr/src/app/debug.txt", "a") as f:
                    f.write(f"[LOCAL DEBUG] | CYCLE {last_rec.cycle_number} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] | traveling_area_handler.py #549 | ADD DUMP SECONDS VALUE {dump_seconds}s | PREVIOUS SEGMENT IS {last_rec.current_segment}\n")

        updated_rec = CycleRecord(**base_params)

        return updated_rec, None
