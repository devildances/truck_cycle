import logging
from datetime import datetime, timezone
from typing import Optional, Tuple

from config.static_config import (IDLE_THRESHOLD_FOR_START_DUMP,
                                  IDLE_THRESHOLD_FOR_START_LOAD,
                                  IDLE_THRESHOLD_IN_TRAVEL_SEGMENT)
from models.dto.method_dto import CycleComparisonContext
from models.dto.record_dto import CycleRecord
from utils.utilities import timestamp_to_utc_zero

logger = logging.getLogger(__name__)


def cycle_loading_area(
    context: CycleComparisonContext
) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
    curr_rec = context.current_record
    last_rec = context.last_record
    loader_rec = context.loader_asset
    params_new_cycle_rec = None
    params_updated_rec = None

    if (curr_rec.work_state_id == 3) and (last_rec.current_work_state_id == 3):
        """This condition where the cycle/haul truck is near a loader but still moving around"""
        if last_rec.current_segment == "EMPTY_TRAVEL":
            """Previous record coming from normal transition"""
            pass
        elif last_rec.current_segment is None:
            """Previous record is initialization record"""
            if last_rec.loader_asset_guid is None:
                pass
            else:
                pass
        else:
            """Previous record coming from abnormal transition"""
            pass
    elif (curr_rec.work_state_id == 2) and (last_rec.current_work_state_id == 2):
        """This condition where the cycle/haul truck is idling near a loader for period of time"""
        if last_rec.load_start_utc is None:
            idling_duration = (
                datetime.now(timezone.utc) - timestamp_to_utc_zero(last_rec.updated_date)
                )
            duration_in_sec = idling_duration.total_seconds()
            if duration_in_sec > IDLE_THRESHOLD_FOR_START_LOAD:
                """At this point, new cycle is created and previous cycle is closed"""
                if last_rec.current_segment:
                    pass
                if last_rec.current_segment == "LOAD_TRAVEL":
                    """Close previous cycle for invalid cycle transition"""
                    if last_rec.load_end_utc:
                        load_travel_dur = timestamp_to_utc_zero(last_rec.updated_date) - timestamp_to_utc_zero(last_rec.load_end_utc)
                        load_travel_seconds = load_travel_dur.total_seconds()
                    else:
                        load_travel_seconds = None
                    params_updated_rec = {}
                elif last_rec.current_segment == "EMPTY_TRAVEL":
                    if (last_rec.load_seconds is None) or (last_rec.load_travel_seconds is None) or (last_rec.dump_end_utc is None):
                        """Close previous cycle for invalid cycle transition"""
                        if last_rec.dump_end_utc:
                            empty_travel_dur = timestamp_to_utc_zero(last_rec.updated_date) - timestamp_to_utc_zero(last_rec.dump_end_utc)
                            empty_travel_seconds = empty_travel_dur.total_seconds()
                        else:
                            empty_travel_seconds = None
                        params_updated_rec = {}
                    else:
                        """Close previous cycle for normal cycle transition"""
                        empty_travel_dur = timestamp_to_utc_zero(last_rec.updated_date) - timestamp_to_utc_zero(last_rec.dump_end_utc)
                        empty_travel_seconds = empty_travel_dur.total_seconds()
                        total_cycle_duration = last_rec.load_seconds + last_rec.load_travel_seconds + last_rec.dump_seconds + empty_travel_seconds
                        params_updated_rec = {}
                elif last_rec.current_segment is None:
                    """Update cycle status of previous initial record"""
                    params_updated_rec = {}
            else:
                params_updated_rec = None
        else:
            params_updated_rec = None
    elif (last_rec.current_work_state_id == 3) and (curr_rec.work_state_id == 2):
        params_updated_rec = {}
    elif (last_rec.current_work_state_id == 2) and (curr_rec.work_state_id == 3):
        if last_rec.current_segment is None:
            params_updated_rec = {}
        elif (last_rec.current_segment == "LOAD_TIME") and last_rec.load_start_utc:
            load_time_dur = datetime.now(timezone.utc) - timestamp_to_utc_zero(last_rec.load_start_utc)
            load_seconds = load_time_dur.total_seconds()
            params_updated_rec = {}
        else:
            params_updated_rec = {}

    updated_rec = CycleRecord(**params_updated_rec) if params_updated_rec else None
    new_cycle_rec = CycleRecord(**params_new_cycle_rec) if params_new_cycle_rec else None

    return (updated_rec, new_cycle_rec,)


def cycle_traveling_area(
    context: CycleComparisonContext
) -> Tuple[Optional[CycleRecord], None]:
    curr_rec = context.current_record
    last_rec = context.last_record
    params_updated_rec = None
    params_new_cycle_rec = None

    if (last_rec.current_work_state_id == 2) and (curr_rec.work_state_id == 2):
        # check if idling more than max default threshold, if yes then we can mark as OUTLIER
        idling_duration = datetime.now(timezone.utc) - timestamp_to_utc_zero(last_rec.updated_date)
        idling_seconds = idling_duration.total_seconds()
        if idling_seconds > IDLE_THRESHOLD_IN_TRAVEL_SEGMENT:
            if not last_rec.is_outlier:
                # mark cycle as OUTLIER and set total_cycle_seconds, outlier_position_latitude and outlier_position_longitude values
                params_updated_rec = {}
            else:
                # set total_cycle_seconds
                params_updated_rec = {}
        else:
            # don't do anything
            params_updated_rec = {}
    elif (last_rec.current_work_state_id == 3) and (curr_rec.work_state_id == 2):
        # just update work_state_id
        params_updated_rec = {}
    elif (last_rec.current_work_state_id == 2) and (curr_rec.work_state_id == 3):
        if last_rec.is_outlier:
            # close current cycle (set cycle_status as OUTLIER) and create new cycle
            params_updated_rec = {}
            params_new_cycle_rec = {}
        else:
            # just update work_state_id
            params_updated_rec = {}
    else:
        # don't do anything
        params_updated_rec = None

    updated_rec = CycleRecord(**params_updated_rec) if params_updated_rec else None
    new_cycle_rec = CycleRecord(**params_new_cycle_rec) if params_new_cycle_rec else None

    return (updated_rec, new_cycle_rec,)


def cycle_dumping_area(
    context: CycleComparisonContext
) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
    curr_rec = context.current_record
    last_rec = context.last_record
    dump_region_rec = context.dump_region
    params_updated_rec = None

    if (last_rec.current_work_state_id == 2) and (curr_rec.work_state_id == 2):
        # check if idling more than max default threshold, if yes then mark as DUMP_TIME
        idle_duration = datetime.now(timezone.utc) - timestamp_to_utc_zero(last_rec.updated_date)
        total_idle_seconds = idle_duration.total_seconds()
        if total_idle_seconds > IDLE_THRESHOLD_FOR_START_DUMP:
            if last_rec.current_segment == "LOAD_TRAVEL":
                # change segment to DUMP_TIME, update dump_start_utc and calculate LOAD_TRAVEL total seconds
                if last_rec.load_end_utc:
                    load_travel_duration = timestamp_to_utc_zero(last_rec.updated_date) - timestamp_to_utc_zero(last_rec.load_end_utc)
                    load_travel_seconds = load_travel_duration.total_seconds()
                else:
                    load_travel_seconds = None
                params_updated_rec = {}
            elif last_rec.current_segment == "DUMP_TIME":
                # do nothing
                params_updated_rec = None
            else:
                # change segment to DUMP_TIME, update dump_start_utc and update work_state_id
                params_updated_rec = {}
        else:
            # do nothing
            params_updated_rec = None
    elif (last_rec.current_work_state_id == 3) and (curr_rec.work_state_id == 2):
        # just update work_state_id
        params_updated_rec = {}
    elif (last_rec.current_work_state_id == 2) and (curr_rec.work_state_id == 3):
        # check if the previous current_segment is DUMP_TIME or not, if yes then change it as EMPTY_TRAVEL
        if last_rec.current_segment == "DUMP_TIME":
            # change segment to EMPTY_TRAVEL, update dump_end_utc and calculate DUMP_TIME total seconds
            dump_time_duration = datetime.now(timezone.utc) - timestamp_to_utc_zero(last_rec.dump_start_utc)
            dump_time_seconds = dump_time_duration.total_seconds()
            params_updated_rec = {}
        else:
            # just update work_state_id
            params_updated_rec = {}
    else:
        # didn't do anything
        params_updated_rec = None

    updated_rec = CycleRecord(**params_updated_rec) if params_updated_rec else None

    return (updated_rec, None,)


def cycle_records_comparison(
    context: CycleComparisonContext
) -> Tuple[Optional[CycleRecord], Optional[CycleRecord]]:
    if context.asset_position == "loading_area":
        return cycle_loading_area(context)
    elif context.asset_position == "dumping_area":
        return cycle_dumping_area(context)
    elif context.asset_position == "traveling_area":
        return cycle_traveling_area(context)