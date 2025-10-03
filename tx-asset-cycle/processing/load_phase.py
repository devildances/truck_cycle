import logging
from typing import Optional

from config.static_config import CYCLES_FOR_FINAL_TABLE
from models.dao.pgsql_action_dao import PgsqlActionDAO
from models.dto.record_dto import CycleRecord, ProcessInfoRecord
from utils import checkpoint

logger = logging.getLogger(__name__)


def load_process(
        pgsql_conn: PgsqlActionDAO,
        service_type: str,
        initial_cycle_record: Optional[CycleRecord],
        last_cycle_record_updated: Optional[CycleRecord],
        new_cycle_record: Optional[CycleRecord],
        process_info_record: Optional[ProcessInfoRecord]
) -> None:
    if initial_cycle_record:
        pgsql_conn.insert_new_data(initial_cycle_record)

    if last_cycle_record_updated:
        pgsql_conn.update_data(last_cycle_record_updated)
        if last_cycle_record_updated.cycle_status in CYCLES_FOR_FINAL_TABLE:
            pgsql_conn.insert_new_final_cycle_data(
                last_cycle_record_updated
            )

    if new_cycle_record:
        pgsql_conn.insert_new_data(new_cycle_record)

    if service_type == "realtime":
        if process_info_record:
            if process_info_record.is_new_record():
                pgsql_conn.insert_new_data(process_info_record)
            else:
                pgsql_conn.update_data(process_info_record)

            target_key = process_info_record.asset_guid
            last_pi = process_info_record.process_date
            if not checkpoint.LATEST_PROCESS_INFO:
                checkpoint.LATEST_PROCESS_INFO = {}
            checkpoint.LATEST_PROCESS_INFO[target_key] = last_pi
            logger.info(f"LATEST_PROCESS_INFO: {checkpoint.LATEST_PROCESS_INFO}")
