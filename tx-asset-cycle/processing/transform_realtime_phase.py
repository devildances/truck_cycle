import logging
import time

from cores.segment_decision import cycle_records_comparison

from config.static_config import LOADER_ASSET_TYPE_GUID
from models.dao.pgsql_action_dao import PgsqlActionDAO
from models.dao.redis_source_dao import RedisSourceDAO
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import RealtimeRecord
from models.query.custom_queries import get_loaders_query
from utils.utilities import (format_log_elapse_time,
                             parse_iso_timestamp_to_naive_utc)

from .preprocess import (get_all_loaders_in_site, get_last_cycle_details,
                         get_latest_process_info, is_current_record_older,
                         is_the_asset_cycle_truck, is_truck_near_any_loader,
                         record_initialization, get_all_dump_regions,
                         is_truck_within_dump_region)

logger = logging.getLogger(__name__)


def process_new_record(
    pgsql_db_conn_obj: PgsqlActionDAO,
    redis_db_conn_obj: RedisSourceDAO,
    current_record_data: dict
) -> tuple:
    main_start_time = time.perf_counter()
    logger.info("Start processing new record data..")

    # STAGE 1 - Parse Raw Record
    curr_rec = RealtimeRecord(
        asset_guid=current_record_data["asset_guid"],
        site_guid=current_record_data["site_guid"],
        timestamp=parse_iso_timestamp_to_naive_utc(
            current_record_data["timestamp"]
        ),
        work_state_id=current_record_data["work_state_id"],
        longitude=float(current_record_data["longitude"]),
        latitude=float(current_record_data["latitude"])
    )

    # STAGE 2 - Check if The Asset is Cycle/Haul Truck
    if not is_the_asset_cycle_truck(
        pgsql_db_conn_obj,
        curr_rec.asset_guid,
        curr_rec.site_guid
    ):
        return (None, None, None, None,)

    # STAGE 3 - Get Latest Process Date and Compare with Current Record
    if is_current_record_older(pgsql_db_conn_obj, curr_rec):
        return (None, None, None, None,)

    # STAGE 4 - Get Last Cycle Details Information of The Current Asset
    last_cycle_rec = get_last_cycle_details(
        pgsql_conn=pgsql_db_conn_obj,
        site_guid=curr_rec.site_guid,
        asset_guid=curr_rec.asset_guid
    )
    is_init_record = False if last_cycle_rec else True

    # STAGE 5A - Check if The Current Record Location is Near Any Loader
    site_loaders = get_all_loaders_in_site(
        pgsql_conn=pgsql_db_conn_obj,
        target_query=get_loaders_query(),
        target_params=[
            LOADER_ASSET_TYPE_GUID,
            curr_rec.site_guid
        ]
    )

    nearest_loader, loader_distance = is_truck_near_any_loader(
        truck_location=[curr_rec.latitude, curr_rec.longitude],
        list_of_loaders=site_loaders
    )

    # STAGE 5B - Check if The Current Record Location is in Any Dump Region
    within_dump_region = None
    if not nearest_loader:
        dump_regions = get_all_dump_regions(
            redis_conn=redis_db_conn_obj,
            site_guid=curr_rec.site_guid
        )
        within_dump_region = is_truck_within_dump_region(
            truck_location=[curr_rec.latitude, curr_rec.longitude],
            list_of_dump_region=dump_regions
        )

    # STAGE 6 - Decision Logic for Current Record Segment Compare to Last Record
    init_record = None
    latest_record_updated = None
    new_cycle_record = None
    pi_record = None

    if is_init_record:
        init_record = record_initialization(current_record=curr_rec)
    else:
        if nearest_loader:
            position = "loading_area"
        elif within_dump_region:
            position = "dumping_area"
        else:
            position = "traveling_area"
        comparison_context = CycleComparisonContext(
            current_record=curr_rec,
            last_record=last_cycle_rec,
            asset_position=position,
            loader_asset=nearest_loader,
            loader_distance=loader_distance,
            dump_region=within_dump_region
        )
        latest_record_updated, new_cycle_record = cycle_records_comparison(
            context=comparison_context
        )

    # STAGE 7 - Get Process Info Record and ID for Insert/Update Purpose
    if is_init_record or latest_record_updated or new_cycle_record:
        pi_record = get_latest_process_info(
            pgsql_conn=pgsql_db_conn_obj,
            asset_guid=curr_rec.asset_guid,
            site_guid=curr_rec.site_guid,
            current_record_timestamp=curr_rec.timestamp
        )

    main_end_time = time.perf_counter()
    logger.info(
        "Elapsed time for TRANSFORMING PHASE: "
        f"{format_log_elapse_time(main_end_time - main_start_time)}"
    )

    return (
        init_record,
        latest_record_updated,
        new_cycle_record,
        pi_record,
    )
