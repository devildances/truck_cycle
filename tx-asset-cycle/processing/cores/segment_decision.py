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
from typing import Optional, Self, Tuple

from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord

from .all_asset_in_dump_handler import AllAssetsInSameDumpAreaHandler
from .dumping_area_handler import DumpingAreaHandler
from .loading_area_handler import LoadingAreaHandler
from .traveling_area_handler import TravelingAreaHandler

logger = logging.getLogger(__name__)


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
