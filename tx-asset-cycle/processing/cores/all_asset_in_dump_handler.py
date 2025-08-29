import logging
from typing import Optional, Self, Tuple

from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord
from utils.utilities import DurationCalculator

from .identifiers import CycleRecordFactory, CycleStateHandler, CycleStatus

logger = logging.getLogger(__name__)


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
