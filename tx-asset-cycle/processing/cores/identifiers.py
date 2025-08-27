import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum, IntEnum
from typing import Dict, Optional, Self, Tuple

from models.dto.asset_dto import LoaderAsset
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord, RealtimeRecord
from utils.utilities import timestamp_to_utc_zero

logger = logging.getLogger(__name__)


class WorkState(IntEnum):
    """Work state identifiers for haul trucks.

    Represents the operational state of a haul truck at any given time.
    These states are used to track transitions and determine cycle segments.

    Attributes:
        IDLING: Truck is stationary (value=2)
        WORKING: Truck is in motion (value=3)
    """
    IDLING = 2
    WORKING = 3


class CycleStatus(str, Enum):
    """Cycle status values for tracking cycle completion.

    Represents the completion status of a haul truck cycle from loading
    to dumping and back.

    Attributes:
        - COMPLETE: All four segments executed with valid durations
        - INVALID: Missing segments or abnormal transitions
        - INPROGRESS: Cycle currently being executed
        - OUTLIER: Cycle terminated due to extended idle in travel segments
    """
    INPROGRESS = "INPROGRESS"
    COMPLETE = "COMPLETE"
    INVALID = "INVALID"
    OUTLIER = "OUTLIER"


class CycleSegment(str, Enum):
    """Cycle segment identifiers for tracking truck position in cycle.

    Represents the current segment of the haul cycle that the truck
    is executing. The normal cycle progression is:
    LOAD_TIME -> LOAD_TRAVEL -> DUMP_TIME -> EMPTY_TRAVEL -> LOAD_TIME

    Abnormal transitions (e.g., LOAD_TRAVEL -> LOAD_TIME) indicate
    the truck dumped outside designated dump regions, skipping DUMP_TIME
    and EMPTY_TRAVEL segments.

    Attributes:
        LOAD_TIME: Truck is being loaded at loader
        LOAD_TRAVEL: Truck is traveling loaded from loader to dump region
        DUMP_TIME: Truck is dumping material at designated dump region
        EMPTY_TRAVEL: Truck is traveling empty from dump region back to loader
    """
    LOAD_TIME = "LOAD_TIME"
    LOAD_TRAVEL = "LOAD_TRAVEL"
    DUMP_TIME = "DUMP_TIME"
    EMPTY_TRAVEL = "EMPTY_TRAVEL"


class CycleRecordFactory:
    """Factory for creating cycle record parameters.

    This factory class provides methods to create parameter dictionaries
    for CycleRecord objects, ensuring consistency and reducing code
    duplication across the module.

    The factory handles two main scenarios:
    1. Creating parameters for updating existing cycle records
    2. Creating parameters for new cycle records

    Example:
        >>> factory = CycleRecordFactory()
        >>> params = factory.create_base_params(last, current)
        >>> record = CycleRecord(**params)
    """

    @staticmethod
    def create_base_params(
        last_rec: CycleRecord,
        curr_rec: RealtimeRecord,
    ) -> Dict:
        """Create base parameters common to most record updates.

        Generates a dictionary of parameters that are typically needed when
        updating an existing cycle record. This includes preserving most
        fields from the last record while updating timestamps and distances.

        Args:
            last_rec: The previous cycle record containing historical data
            curr_rec: The current cycle record with new state information

        Returns:
            Dict: A dictionary of parameters ready to create a CycleRecord

        Note:
            The returned dictionary includes an updated timestamp set to
            the current UTC time.
        """
        return {
            "asset_cycle_vlx_id": last_rec.asset_cycle_vlx_id,
            "asset_guid": last_rec.asset_guid,
            "cycle_number": last_rec.cycle_number,
            "cycle_status": last_rec.cycle_status,
            "current_process_date": curr_rec.timestamp,
            "current_area": curr_rec.current_area,
            "site_guid": last_rec.site_guid,
            "current_segment": last_rec.current_segment,
            "previous_work_state_id": last_rec.current_work_state_id,
            "current_work_state_id": curr_rec.work_state_id,
            "loader_asset_guid": last_rec.loader_asset_guid,
            "loader_latitude": last_rec.loader_latitude,
            "loader_longitude": last_rec.loader_longitude,
            "previous_loader_distance": last_rec.previous_loader_distance,
            "current_loader_distance": last_rec.current_loader_distance,
            "idle_in_dump_region_guid": last_rec.idle_in_dump_region_guid,
            "dump_region_guid": last_rec.dump_region_guid,
            "all_assets_in_same_dump_area": (
                last_rec.all_assets_in_same_dump_area
            ),
            "is_outlier": last_rec.is_outlier,
            "outlier_position_latitude": last_rec.outlier_position_latitude,
            "outlier_position_longitude": last_rec.outlier_position_longitude,
            "load_start_utc": last_rec.load_start_utc,
            "load_end_utc": last_rec.load_end_utc,
            "dump_start_utc": last_rec.dump_start_utc,
            "dump_end_utc": last_rec.dump_end_utc,
            "load_travel_seconds": last_rec.load_travel_seconds,
            "empty_travel_seconds": last_rec.empty_travel_seconds,
            "dump_seconds": last_rec.dump_seconds,
            "load_seconds": last_rec.load_seconds,
            "total_cycle_seconds": last_rec.total_cycle_seconds,
            "outlier_seconds": last_rec.outlier_seconds,
            "cycle_start_utc": last_rec.cycle_start_utc,
            "cycle_end_utc": last_rec.cycle_end_utc,
            "created_date": timestamp_to_utc_zero(last_rec.created_date),
            "updated_date": datetime.now(timezone.utc)
        }

    @staticmethod
    def create_new_cycle_params(
        curr_rec: RealtimeRecord,
        last_rec: CycleRecord,
        context: CycleComparisonContext,
        loader_rec: Optional[LoaderAsset] = None,
    ) -> Dict:
        """Create parameters for a new cycle record.

        Generates a dictionary of parameters for initiating a new cycle.
        This is typically called when a truck begins loading after completing
        a previous cycle or when starting operations.

        Args:
            curr_rec: Current record with truck's current state
            last_rec: Previous record used to increment cycle number
            loader_rec: Loader asset record for position information
            context: Comparison context with distance information

        Returns:
            Dict: Parameters for creating a new CycleRecord

        Note:
            - Cycle number is incremented from the last record
            - All duration fields are initialized to None
            - Status is set to INPROGRESS
            - Segment is set to LOAD_TIME
        """
        return {
            "asset_guid": curr_rec.asset_guid,
            "cycle_number": last_rec.cycle_number + 1,
            "cycle_status": CycleStatus.INPROGRESS.value,
            "current_process_date": curr_rec.timestamp,
            "current_area": curr_rec.current_area,
            "site_guid": curr_rec.site_guid,
            "current_segment": CycleSegment.LOAD_TIME.value,
            "previous_work_state_id": None,
            "current_work_state_id": curr_rec.work_state_id,
            "loader_asset_guid": (
                loader_rec.asset_guid if loader_rec else None
            ),
            "loader_latitude": (
                loader_rec.latitude if loader_rec else None
            ),
            "loader_longitude": (
                loader_rec.longitude if loader_rec else None
            ),
            "previous_loader_distance": None,
            "current_loader_distance": context.loader_distance,
            "idle_in_dump_region_guid": None,
            "dump_region_guid": None,
            "all_assets_in_same_dump_area": False,
            "is_outlier": False,
            "outlier_position_latitude": None,
            "outlier_position_longitude": None,
            "load_start_utc": (
                timestamp_to_utc_zero(
                    last_rec.current_process_date
                ) if loader_rec else None
            ),
            "load_end_utc": None,
            "dump_start_utc": None,
            "dump_end_utc": None,
            "load_travel_seconds": None,
            "empty_travel_seconds": None,
            "dump_seconds": None,
            "load_seconds": None,
            "total_cycle_seconds": None,
            "outlier_seconds": None,
            "cycle_start_utc": last_rec.current_process_date,
            "cycle_end_utc": None,
            "created_date": datetime.now(timezone.utc),
            "updated_date": datetime.now(timezone.utc)
        }

    @staticmethod
    def create_outlier_recovery_params(
        curr_rec: RealtimeRecord,
        last_rec: CycleRecord
    ) -> Dict:
        """Create parameters for a new cycle after closing an outlier.

        Creates a minimal cycle record to continue tracking after an
        outlier cycle is closed. Most fields are set to None as the
        truck's position in the cycle flow is unknown. This is different
        from create_new_cycle_params which is used in loading area and
        requires loader information.

        Args:
            curr_rec: Current realtime record with truck's current state
            last_rec: Previous cycle record (the outlier being closed)

        Returns:
            Dict: Parameters for creating a new CycleRecord with minimal
                  information and no loader/dump region associations

        Note:
            - No loader information is set (unlike loading area new cycles)
            - current_segment is None as position in cycle is unknown
            - All time and duration fields are None
            - is_outlier is set to False for the new cycle
        """
        return {
            "asset_guid": curr_rec.asset_guid,
            "cycle_number": last_rec.cycle_number + 1,
            "cycle_status": CycleStatus.INPROGRESS.value,
            "current_process_date": curr_rec.timestamp,
            "current_area": curr_rec.current_area,
            "site_guid": curr_rec.site_guid,
            "current_segment": None,
            "previous_work_state_id": None,
            "current_work_state_id": curr_rec.work_state_id,
            "loader_asset_guid": None,
            "loader_latitude": None,
            "loader_longitude": None,
            "previous_loader_distance": None,
            "current_loader_distance": None,
            "idle_in_dump_region_guid": None,
            "dump_region_guid": None,
            "all_assets_in_same_dump_area": False,
            "is_outlier": False,
            "outlier_position_latitude": None,
            "outlier_position_longitude": None,
            "load_start_utc": None,
            "load_end_utc": None,
            "dump_start_utc": None,
            "dump_end_utc": None,
            "load_travel_seconds": None,
            "empty_travel_seconds": None,
            "dump_seconds": None,
            "load_seconds": None,
            "total_cycle_seconds": None,
            "outlier_seconds": None,
            "cycle_start_utc": curr_rec.timestamp,
            "cycle_end_utc": None,
            "created_date": datetime.now(timezone.utc),
            "updated_date": datetime.now(timezone.utc)
        }


class CycleStateHandler(ABC):
    """Abstract base class for cycle state handlers.

    Defines the interface for handling cycle state transitions in different
    operational areas (loading, dumping, traveling). Each concrete handler
    implements area-specific logic for processing cycle records.

    Subclasses must implement the handle() method to process state
    transitions and return appropriate record updates.

    Example:
        >>> class CustomHandler(CycleStateHandler):
        ...     def handle(self, context):
        ...         # Implementation here
        ...         return updated_record, new_record
    """

    @abstractmethod
    def handle(
        self: Self, context: CycleComparisonContext
    ) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
        """Handle state transition and return updated/new records.

        Process the given context to determine if cycle records need
        to be updated or if new cycles should be created.

        Args:
            context: The cycle comparison context containing current
                    and previous records, asset position, and other
                    relevant information

        Returns:
            Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
                - First element: Updated version of the last record (if any)
                - Second element: New cycle record to create (if any)
                Either or both elements can be None

        Raises:
            NotImplementedError: This is an abstract method that must
                                 be implemented by subclasses
        """
        pass
