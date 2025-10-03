import logging

from models.dao.pgsql_action_dao import PgsqlActionDAO
from models.dao.redis_source_dao import RedisSourceDAO
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import RealtimeRecord
from utils.utilities import parse_iso_timestamp_to_naive_utc

from .cores.identifiers import CycleSegment
from .cores.segment_decision import cycle_records_comparison
from .preprocess import (get_all_loaders_in_site, get_all_regions,
                         get_last_cycle_details, get_latest_process_info,
                         get_nearest_load_region,
                         get_next_cycle_number_for_reprocess, get_site_guid,
                         is_asset_within_dump_region, is_current_record_older,
                         is_the_asset_cycle_truck, is_truck_near_any_loader,
                         record_initialization)

logger = logging.getLogger(__name__)


def process_new_record(
    pgsql_db_conn_obj: PgsqlActionDAO,
    redis_db_conn_obj: RedisSourceDAO,
    current_record_data: dict,
    service_type: str
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
    if service_type == "realtime":
        if is_current_record_older(pgsql_db_conn_obj, curr_rec):
            return (None, None, None, None,)

    # STAGE 5 - Get Last Cycle Details Information of The Current Asset
    last_cycle_rec = get_last_cycle_details(
        pgsql_conn=pgsql_db_conn_obj,
        site_guid=curr_rec.site_guid,
        asset_guid=curr_rec.asset_guid,
        service_type=service_type
    )
    is_init_record = False if last_cycle_rec else True


    cycle_num = 1 if is_init_record else last_cycle_rec.cycle_number
    tt = "IDLING" if curr_rec.work_state_id == 2 else "WORKING"
    if is_init_record:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"\n\n[LOCAL DEBUG] | PREVIOUS CYCLE RECORD INFO | NO PREVIOUS CYCLE RECORD INFO! THIS IS FIRST STREAMED DATA BEING PROCESSED\n")
    else:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(
                "\n\n[LOCAL DEBUG] | PREVIOUS CYCLE RECORD INFO | "
                f"RECORD DATE: {last_cycle_rec.current_process_date} | "
                f"AREA: {last_cycle_rec.current_area} | "
                f"SEGMENT: {last_cycle_rec.current_segment} | "
                f"STATE: {'IDLING' if last_cycle_rec.current_work_state_id == 2 else 'WORKING'} | "
                f"POSITION: [{last_cycle_rec.current_asset_latitude}, {last_cycle_rec.current_asset_longitude}] | "
                f"IS OUTLIER: {last_cycle_rec.is_outlier}\n"
            )
    with open("/usr/src/app/debug.txt", "a") as f:
        f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | START PROCESSING NEW RECORD \n")

    # STAGE 6A - Check if The Current Record Location is Near Any Loader
    site_loaders = get_all_loaders_in_site(
        pgsql_conn=pgsql_db_conn_obj,
        service_type=service_type,
        target_params=[
            curr_rec.site_guid,
            curr_rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        ]
    )

    nearest_loader, loader_distance = is_truck_near_any_loader(
        truck_location=[curr_rec.latitude, curr_rec.longitude],
        list_of_loaders=site_loaders
    )

    load_regions = None
    nearest_load_region = None
    truck_within_load_region = False
    distance_truck_with_load_region = None

    if nearest_loader:
        load_regions = get_all_regions(
            redis_conn=redis_db_conn_obj,
            site_guid=curr_rec.site_guid,
            region_type="load"
        )
        if load_regions:
            (
                nearest_load_region,
                truck_within_load_region,
                distance_truck_with_load_region
            ) = get_nearest_load_region(
                asset_location=[curr_rec.latitude, curr_rec.longitude],
                list_of_load_regions=load_regions
            )
            distance_truck_with_load_region = (
                0.0 if truck_within_load_region else distance_truck_with_load_region
            )

    from utils.utilities import haversine_distance
    if site_loaders:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | TOTAL LOADERS IN SITE {curr_rec.site_guid}: {len(site_loaders)} unit(s)\n")
        for i in site_loaders:
            loaderdistance = haversine_distance(point1=[curr_rec.latitude, curr_rec.longitude], point2=[i.latitude, i.longitude])
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | LOADER {i.asset_guid} [{i.latitude}, {i.longitude}] {loaderdistance}m FROM HAUL TRUCK\n")
        if nearest_loader:
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK IS NEAR WITH LOADER UNIT {nearest_loader.asset_guid} WITH DISTANCE {loader_distance}m\n")
                if load_regions:
                    with open("/usr/src/app/debug.txt", "a") as f:
                        f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | TOTAL LOAD AREA IN SITE {curr_rec.site_guid}: {len(load_regions)} area(s)\n")
                    for k in load_regions:
                        with open("/usr/src/app/debug.txt", "a") as f:
                            f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | LOAD AREA {k.region_guid} {k.region_points}\n")
                    if nearest_load_region:
                        if truck_within_load_region:
                            with open("/usr/src/app/debug.txt", "a") as f:
                                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK ALSO IS IN LOAD AREA {nearest_load_region.region_guid}\n")
                        else:
                            with open("/usr/src/app/debug.txt", "a") as f:
                                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK IS NOT IN ANY LOAD AREA BUT THE NEAREST AREA IS {nearest_load_region.region_guid} WITH DISTANCE {distance_truck_with_load_region}m\n")
                else:
                    with open("/usr/src/app/debug.txt", "a") as f:
                        f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | NO LOAD AREA FOUND IN SITE {curr_rec.site_guid}\n")
        else:
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK IS NOT NEAR WITH ANY LOADER UNIT\n")
    else:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | NO LOADER FOUND IN SITE {curr_rec.site_guid}\n")

    # STAGE 6B - Check if The Current Record Location is in Any Dump Region
    dump_regions = get_all_regions(
        redis_conn=redis_db_conn_obj,
        site_guid=curr_rec.site_guid,
        region_type="dump"
    )
    truck_within_dump_region = is_asset_within_dump_region(
        asset_type="truck",
        asset_location=[curr_rec.latitude, curr_rec.longitude],
        list_of_dump_regions=dump_regions
    )

    if dump_regions:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | TOTAL DUMP AREA IN SITE {curr_rec.site_guid}: {len(dump_regions)} area(s)\n")
        for j in dump_regions:
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | DUMP AREA {j.region_guid} [{j.region_points}]\n")
        if truck_within_dump_region:
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK IS IN DUMP AREA {truck_within_dump_region.region_guid}\n")
        else:
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK IS NOT IN ANY DUMP AREA\n")
    else:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | NO DUMP AREA FOUND IN SITE {curr_rec.site_guid}\n")

    # STAGE 6C - Check if the nearest loader is in the same Dump Region with Haul Truck
    assets_in_same_location = False
    if nearest_loader and truck_within_dump_region:
        loader_within_dump_region = is_asset_within_dump_region(
            asset_type="loader",
            asset_location=[nearest_loader.latitude, nearest_loader.longitude],
            list_of_dump_regions=[truck_within_dump_region]
        )
        if last_cycle_rec.current_segment in [
            CycleSegment.LOAD_TIME, CycleSegment.LOAD_TRAVEL
        ]:
            curr_rec.current_area = "DUMP"
            position = "dumping_area"
        else:
            curr_rec.current_area = "LOAD"
            position = "loading_area"
            if loader_within_dump_region:
                with open("/usr/src/app/debug.txt", "a") as f:
                    f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | THE HAUL TRUCK AND AND THE NEAREST LOADER {nearest_loader.asset_guid} ARE IN THE SAME DUMP AREA {loader_within_dump_region.region_guid}\n")
                assets_in_same_location = True
    elif nearest_loader and not truck_within_dump_region:
        curr_rec.current_area = "LOAD"
        position = "loading_area"
    elif not nearest_loader and truck_within_dump_region:
        curr_rec.current_area = "DUMP"
        position = "dumping_area"
    else:
        curr_rec.current_area = "TRAVEL"
        position = "traveling_area"

    # STAGE 7 - Decision Logic for Current Record Segment Compare to Last Record
    init_record = None
    latest_record_updated = None
    new_cycle_record = None
    pi_record = None

    if is_init_record:
        if service_type == "realtime":
            cycle_number = 1
        else:
            cycle_number = 1
            # cycle_number = get_next_cycle_number_for_reprocess(
            #     pgsql_conn=pgsql_db_conn_obj,
            #     target_params=[
            #         curr_rec.asset_guid,
            #         curr_rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            #     ]
            # )
        init_record = record_initialization(
            current_record=curr_rec,
            start_cycle_number=cycle_number
        )
        init_record.SERVICE_TYPE = service_type
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(
                "[LOCAL DEBUG] | CURRENT CYCLE RECORD UPDATED | "
                f"RECORD DATE: {init_record.current_process_date} | "
                f"AREA: {init_record.current_area} | "
                f"SEGMENT: {init_record.current_segment} | "
                f"STATE: {'IDLING' if init_record.current_work_state_id == 2 else 'WORKING'} | "
                f"POSITION: [{init_record.current_asset_latitude}, {init_record.current_asset_longitude}] | "
                f"IS OUTLIER: {init_record.is_outlier} | "
                f"CYCLE STATUS: {init_record.cycle_status}\n"
            )
    else:
        comparison_context = CycleComparisonContext(
            current_record=curr_rec,
            last_record=last_cycle_rec,
            asset_position=position,
            loader_asset=nearest_loader,
            loader_distance=loader_distance,
            dump_region=truck_within_dump_region,
            assets_in_same_location=assets_in_same_location,
            is_truck_within_load_region=truck_within_load_region,
            nearest_load_region=nearest_load_region,
            distance_truck_with_load_region=distance_truck_with_load_region
        )
        latest_record_updated, new_cycle_record = cycle_records_comparison(
            context=comparison_context
        )
        if latest_record_updated:
            latest_record_updated.SERVICE_TYPE = service_type
            with open("/usr/src/app/debug.txt", "a") as f:
                f.write(
                    "[LOCAL DEBUG] | CURRENT CYCLE RECORD UPDATED | "
                    f"RECORD DATE: {latest_record_updated.current_process_date} | "
                    f"AREA: {latest_record_updated.current_area} | "
                    f"SEGMENT: {latest_record_updated.current_segment} | "
                    f"STATE: {'IDLING' if latest_record_updated.current_work_state_id == 2 else 'WORKING'} | "
                    f"POSITION: [{latest_record_updated.current_asset_latitude}, {latest_record_updated.current_asset_longitude}] | "
                    f"IS OUTLIER: {latest_record_updated.is_outlier} | "
                    f"CYCLE STATUS: {latest_record_updated.cycle_status}\n"
                )
        if new_cycle_record:
            new_cycle_record.SERVICE_TYPE = service_type

    # STAGE 8 - Get Process Info Record and ID for Insert/Update Purpose
    if service_type == "realtime":
        if is_init_record or latest_record_updated or new_cycle_record:
            pi_record = get_latest_process_info(
                pgsql_conn=pgsql_db_conn_obj,
                asset_guid=curr_rec.asset_guid,
                site_guid=curr_rec.site_guid,
                current_record_timestamp=curr_rec.timestamp
            )
            
    if new_cycle_record:
        with open("/usr/src/app/debug.txt", "a") as f:
            f.write(f"[LOCAL DEBUG] | CYCLE {cycle_num} | {curr_rec.timestamp} [{curr_rec.latitude}, {curr_rec.longitude}] {tt} | CLOSE CURRENT CYCLE {latest_record_updated.cycle_number} AS {latest_record_updated.cycle_status} AND CREATE NEW CYCLE {new_cycle_record.cycle_number} IN {new_cycle_record.current_segment} SEGMENT\n")

    return (
        init_record,
        latest_record_updated,
        new_cycle_record,
        pi_record,
    )
