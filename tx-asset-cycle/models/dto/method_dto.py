from dataclasses import dataclass
from typing import Optional

from .asset_dto import LoaderAsset
from .record_dto import CycleRecord, RealtimeRecord
from .region_polygon_dto import RegionPoly


@dataclass
class CycleComparisonContext:
    current_record: RealtimeRecord
    last_record: CycleRecord
    asset_position: str
    loader_asset: Optional[LoaderAsset]
    loader_distance: Optional[float]
    dump_region: Optional[RegionPoly]
