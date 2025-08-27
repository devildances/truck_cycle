import logging

from models.dao.pgsql_action_dao import PgsqlActionDAO
from models.dao.redis_source_dao import RedisSourceDAO
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import RealtimeRecord
from models.query.custom_queries import get_loaders_from_process_info_query
from utils.utilities import parse_iso_timestamp_to_naive_utc

from .cores.segment_decision import cycle_records_comparison
from .preprocess import (get_all_dump_regions, get_all_loaders_in_site,
                         get_last_cycle_details, get_latest_process_info,
                         get_site_guid, is_asset_within_dump_region,
                         is_current_record_older, is_the_asset_cycle_truck,
                         is_truck_near_any_loader, record_initialization)

logger = logging.getLogger(__name__)


def process_new_record(
    pgsql_db_conn_obj: PgsqlActionDAO,
    redis_db_conn_obj: RedisSourceDAO,
    current_record_data: dict
) -> tuple:
    # STAGE 1 - Get Site GUID info
    site_guid = None
    if current_record_data["site_guid"]:
        site_guid = current_record_data["site_guid"]
    else:
        site_guid = get_site_guid(
            pgsql_conn=pgsql_db_conn_obj,
            redis_conn=redis_db_conn_obj,
            asset_guid=current_record_data["asset_guid"]
        )
        if not site_guid:
            return (None, None, None, None,)

    # STAGE 2 - Parse Raw Record
    curr_rec = RealtimeRecord(
        asset_guid=current_record_data["asset_guid"],
        site_guid=site_guid,
        timestamp=parse_iso_timestamp_to_naive_utc(
            current_record_data["timestamp"]
        ),
        work_state_id=int(
            current_record_data["work_state_id"]
        ),
        longitude=float(current_record_data["longitude"]),
        latitude=float(current_record_data["latitude"])
    )

    # STAGE 3 - Check if The Asset is Cycle/Haul Truck
    if not is_the_asset_cycle_truck(
        pgsql_db_conn_obj,
        curr_rec.asset_guid,
        curr_rec.site_guid
    ):
        return (None, None, None, None,)

    # STAGE 4 - Get Latest Process Date and Compare with Current Record
    if is_current_record_older(pgsql_db_conn_obj, curr_rec):
        return (None, None, None, None,)

    # STAGE 5 - Get Last Cycle Details Information of The Current Asset
    last_cycle_rec = get_last_cycle_details(
        pgsql_conn=pgsql_db_conn_obj,
        site_guid=curr_rec.site_guid,
        asset_guid=curr_rec.asset_guid
    )
    is_init_record = False if last_cycle_rec else True

    # STAGE 6A - Check if The Current Record Location is Near Any Loader
    site_loaders = get_all_loaders_in_site(
        pgsql_conn=pgsql_db_conn_obj,
        target_query=get_loaders_from_process_info_query(),
        target_params=[curr_rec.site_guid]
    )

    nearest_loader, loader_distance = is_truck_near_any_loader(
        truck_location=[curr_rec.latitude, curr_rec.longitude],
        list_of_loaders=site_loaders
    )

    # STAGE 6B - Check if The Current Record Location is in Any Dump Region
    dump_regions = get_all_dump_regions(
        redis_conn=redis_db_conn_obj,
        site_guid=curr_rec.site_guid
    )
    truck_within_dump_region = is_asset_within_dump_region(
        asset_type="truck",
        asset_location=[curr_rec.latitude, curr_rec.longitude],
        list_of_dump_regions=dump_regions
    )

    # STAGE 6C - Check if the nearest loader is in the same Dump Region with Haul Truck
    assets_in_same_location = False
    if nearest_loader:
        if truck_within_dump_region:
            loader_within_dump_region = is_asset_within_dump_region(
                asset_type="loader",
                asset_location=[nearest_loader.latitude, nearest_loader.longitude],
                list_of_dump_regions=[truck_within_dump_region]
            )

            if loader_within_dump_region:
                assets_in_same_location = True

    # STAGE 7 - Decision Logic for Current Record Segment Compare to Last Record
    init_record = None
    latest_record_updated = None
    new_cycle_record = None
    pi_record = None

    if nearest_loader:
        curr_rec.current_area = "LOAD"
        position = "loading_area"
    elif truck_within_dump_region:
        curr_rec.current_area = "DUMP"
        position = "dumping_area"
    else:
        curr_rec.current_area = "TRAVEL"
        position = "traveling_area"

    if is_init_record:
        init_record = record_initialization(current_record=curr_rec)
    else:
        comparison_context = CycleComparisonContext(
            current_record=curr_rec,
            last_record=last_cycle_rec,
            asset_position=position,
            loader_asset=nearest_loader,
            loader_distance=loader_distance,
            dump_region=truck_within_dump_region,
            assets_in_same_location=assets_in_same_location
        )
        latest_record_updated, new_cycle_record = cycle_records_comparison(
            context=comparison_context
        )

    # STAGE 8 - Get Process Info Record and ID for Insert/Update Purpose
    if is_init_record or latest_record_updated or new_cycle_record:
        pi_record = get_latest_process_info(
            pgsql_conn=pgsql_db_conn_obj,
            asset_guid=curr_rec.asset_guid,
            site_guid=curr_rec.site_guid,
            current_record_timestamp=curr_rec.timestamp
        )

    return (
        init_record,
        latest_record_updated,
        new_cycle_record,
        pi_record,
    )
