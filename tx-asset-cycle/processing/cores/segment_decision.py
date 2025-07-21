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
from datetime import datetime, timezone
from typing import Optional, Self, Tuple

from config.static_config import (IDLE_THRESHOLD_FOR_START_DUMP,
                                  IDLE_THRESHOLD_FOR_START_LOAD,
                                  IDLE_THRESHOLD_IN_TRAVEL_SEGMENT)
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import DurationCalculator, timestamp_to_utc_zero

from .identifiers import (CycleRecordFactory, CycleSegment, CycleStateHandler,
                          CycleStatus, WorkState)

logger = logging.getLogger(__name__)


class LoadingAreaHandler(CycleStateHandler):
    """Handler for loading area cycle logic.

    Manages cycle state transitions when a haul truck is in the loading area.
    This includes detecting when loading starts, tracking loading duration,
    and handling transitions to travel segments.

    The handler uses configurable thresholds (like IDLE_THRESHOLD_FOR_START_LOAD)
    to determine when a truck has been idle long enough to start a new loading
    cycle.

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Example:
        >>> handler = LoadingAreaHandler()
        >>> updated, new = handler.handle(context)
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

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record) or (None, None)

        Note:
            Uses IDLE_THRESHOLD_FOR_START_LOAD to determine when to
            start a new cycle.
        """
        last_rec = context.last_record

        if last_rec.load_start_utc is None:
            idle_duration = self.calculator.calculate_idle_duration(
                last_rec.updated_date
            )

            if idle_duration > IDLE_THRESHOLD_FOR_START_LOAD:
                return self._start_new_cycle(context)

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

            load_start = timestamp_to_utc_zero(last_rec.updated_date)

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
                curr_rec, last_rec, loader_rec, context
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

        Args:
            context: Cycle comparison context

        Returns:
            Optional[CycleRecord]: Updated record with closed cycle,
                                 or None if no closing needed
        """
        last_rec = context.last_record
        segment = last_rec.current_segment

        if segment == CycleSegment.LOAD_TRAVEL.value:
            return self._close_load_travel_cycle(context)
        elif segment == CycleSegment.EMPTY_TRAVEL.value:
            return self._close_empty_travel_cycle(context)

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
        """
        last_rec = context.last_record
        base_params = self.factory.create_base_params(
            last_rec, context.current_record
        )

        # Calculate load travel duration
        load_travel_seconds = self.calculator.calculate_seconds(
            last_rec.load_end_utc, last_rec.updated_date
        )

        base_params.update({
            "cycle_status": CycleStatus.INVALID.value,
            "load_travel_seconds": load_travel_seconds,
            "previous_loader_distance": last_rec.current_loader_distance,
            "current_loader_distance": context.loader_distance,
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
            last_rec.dump_end_utc, last_rec.updated_date
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

    def _handle_state_transition(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle work state transitions.

        Processes transitions between WORKING and IDLING states when the
        truck is in the loading area:

        1. WORKING -> IDLING: Truck stops moving, only update work state
        2. IDLING -> WORKING: Truck starts moving
            - If current_segment is None: Only update work state
            - If in LOAD_TIME with valid load_start_utc: Complete loading
                and transition to LOAD_TRAVEL
            - Otherwise: Only update work state

        Args:
            context: Cycle comparison context

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - (updated_record, None) for state transitions
                - (None, None) if no action needed
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

            # If current_segment is None, just update work state
            if last_rec.current_segment is None:
                base_params = self.factory.create_base_params(
                    last_rec, curr_rec
                )

                logger.debug(
                    f"State transition IDLING->WORKING for initial record "
                    f"(cycle {last_rec.cycle_number})"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

            # Check if we need to complete loading and transition to LOAD_TRAVEL
            elif (
                last_rec.current_segment == CycleSegment.LOAD_TIME.value
                and last_rec.load_start_utc is not None
            ):

                # Calculate loading duration
                load_end_time = datetime.now(timezone.utc)
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
                    "previous_loader_distance": last_rec.current_loader_distance,
                    "current_loader_distance": context.loader_distance,
                })

                logger.info(
                    f"Completing LOAD_TIME for cycle {last_rec.cycle_number}: "
                    f"Load duration {load_seconds:.1f}s, transitioning to LOAD_TRAVEL"
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
                    f"{last_rec.cycle_number} in segment {last_rec.current_segment}"
                )

                updated_rec = CycleRecord(**base_params)
                return updated_rec, None

        # No transition detected
        return None, None


class DumpingAreaHandler(CycleStateHandler):
    """Handler for dumping area cycle logic.

    Manages cycle state transitions when a haul truck is in the dumping area.
    This includes detecting when dumping starts, tracking dumping duration,
    and handling transitions to empty travel segments.

    The handler uses configurable thresholds (like IDLE_THRESHOLD_FOR_START_DUMP)
    to determine when a truck has been idle long enough to start dumping.

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Example:
        >>> handler = DumpingAreaHandler()
        >>> updated, _ = handler.handle(context)
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
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle cycle logic in dumping area.

        Processes the cycle comparison context to determine appropriate
        actions based on the truck's current and previous states. This
        includes handling trucks idling for dumping and state transitions.

        Args:
            context: Cycle comparison context with current/previous records

        Returns:
            Tuple of (updated_record, None) - never creates new cycles
            in dumping area

        Note:
            The dumping area handler only updates existing records,
            it never creates new cycles.
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
        configured threshold, it transitions to DUMP_TIME segment.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None) or (None, None)

        Note:
            Uses IDLE_THRESHOLD_FOR_START_DUMP to determine when to
            start dumping.
        """
        last_rec = context.last_record

        # Calculate idle duration
        idle_duration = self.calculator.calculate_idle_duration(
            last_rec.updated_date
        )

        if idle_duration > IDLE_THRESHOLD_FOR_START_DUMP:
            return self._handle_idle_threshold_exceeded(context)

        # Not idle long enough, no action needed
        return None, None

    def _handle_idle_threshold_exceeded(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle when idle threshold is exceeded.

        Determines action based on current segment:
        - LOAD_TRAVEL: Transition to DUMP_TIME and calculate load travel time
        - DUMP_TIME: Already dumping, no action needed
        - Other: Transition to DUMP_TIME

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None) or (None, None)
        """
        last_rec = context.last_record
        segment = last_rec.current_segment

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
        """Transition from LOAD_TRAVEL to DUMP_TIME.

        Updates the cycle record to DUMP_TIME segment, sets dump_start_utc,
        and calculates load_travel_seconds.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None)
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
                last_rec.updated_date
            )

        # Set dump start time
        dump_start = timestamp_to_utc_zero(last_rec.updated_date)

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
        """Transition to DUMP_TIME from other segments.

        Updates the cycle record to DUMP_TIME segment and sets dump_start_utc
        for segments other than LOAD_TRAVEL.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None)
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
        dump_start = timestamp_to_utc_zero(last_rec.updated_date)

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
    ) -> Tuple[Optional[CycleRecord], None]:
        """Handle work state transitions in dumping area.

        Processes transitions between WORKING and IDLING states:

        1. WORKING -> IDLING: Truck stops moving, update work state
        2. IDLING -> WORKING: Truck starts moving
           - If in DUMP_TIME: Complete dumping and transition to EMPTY_TRAVEL
           - Otherwise: Just update work state

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None) or (None, None)
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

            # Check if completing DUMP_TIME
            if last_rec.current_segment == CycleSegment.DUMP_TIME.value:
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

    def _complete_dump_time_to_empty_travel(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[CycleRecord, None]:
        """Complete DUMP_TIME and transition to EMPTY_TRAVEL.

        Calculates dump duration and transitions the cycle to EMPTY_TRAVEL
        segment when the truck starts moving after dumping.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None)
        """
        last_rec = context.last_record
        curr_rec = context.current_record

        # Calculate dump duration
        dump_end_time = datetime.now(timezone.utc)
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
    """Handler for traveling area cycle logic.

    Manages cycle state transitions when a haul truck is traveling between
    loading and dumping areas. This includes detecting outlier behavior when
    trucks idle too long during travel segments.

    The handler uses configurable threshold (IDLE_THRESHOLD_IN_TRAVEL_SEGMENT)
    to determine when a truck has been idle long enough to be marked as outlier.

    Attributes:
        factory: CycleRecordFactory instance for creating records
        calculator: DurationCalculator instance for time calculations

    Example:
        >>> handler = TravelingAreaHandler()
        >>> updated, new = handler.handle(context)
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
        actions based on the truck's current and previous states. This
        includes detecting outlier behavior and handling state transitions.

        Args:
            context: Cycle comparison context with current/previous records

        Returns:
            Tuple of (updated_record, new_cycle_record) where either or
            both can be None. New cycles are created only when closing
            outlier cycles.

        Note:
            Unlike loading/dumping areas, traveling area can create new
            cycles when an outlier cycle needs to be closed.
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
        the travel area. If the truck has been idle for longer than the
        configured threshold, it marks the cycle as outlier.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, None) or (None, None)

        Note:
            Uses IDLE_THRESHOLD_IN_TRAVEL_SEGMENT to determine when to
            mark as outlier. Sets outlier position from current record.
        """
        last_rec = context.last_record

        # Calculate idle duration
        idle_duration = self.calculator.calculate_idle_duration(
            last_rec.updated_date
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
        position where the outlier behavior occurred.

        Args:
            context: Cycle comparison context
            outlier_duration: total seconds for truck idling

        Returns:
            Tuple of (updated_record, None)
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
           - Otherwise: Just update work state

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record) or (None, None)
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
        cycle for the truck to continue operations.

        Args:
            context: Cycle comparison context

        Returns:
            Tuple of (updated_record, new_cycle_record)
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
        }

    def process_context(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Process the context and return updated/new records.

        Routes the context to the appropriate handler based on the
        asset_position field in the context. If no handler is found
        for the position, logs a warning and returns no updates.

        Args:
            context: Cycle comparison context containing asset position

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                Results from the selected handler, or (None, None) if
                no handler found

        Warning:
            Logs a warning if asset_position doesn't match any handler
        """
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
