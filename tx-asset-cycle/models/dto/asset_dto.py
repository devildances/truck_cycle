from dataclasses import dataclass


@dataclass
class LoaderAsset:
    asset_guid: str
    site_guid: str
    latitude: float
    longitude: float


@dataclass
class TruckAsset:
    asset_guid: str
    site_guid: str
    asset_type_guid: str
