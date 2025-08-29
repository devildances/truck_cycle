import logging
from typing import Optional, Self, Tuple

from config.static_config import IDLE_THRESHOLD_IN_TRAVEL_SEGMENT
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import DurationCalculator

from .identifiers import CycleRecordFactory, CycleStateHandler, WorkState

logger = logging.getLogger(__name__)


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
