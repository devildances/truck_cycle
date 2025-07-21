def test():
    # from .preprocess import is_the_asset_cycle_truck
    # from models.query.custom_queries import get_loaders_query
    # from models.dao.pgsql_action_dao import PgsqlActionDAO
    # from models.dto.record_dto import ProcessInfoRecord
    # from datetime import datetime, timezone
    # from utils.utilities import parse_iso_timestamp_to_naive_utc

    # raw = {
    #     "asset_guid": "test_asset_guid",
    #     "site_guid": "test_site_guid",
    #     "process_name": "test_process_name",
    #     "process_date": parse_iso_timestamp_to_naive_utc("2025-07-16 12:49:37"),
    #     "created_date": parse_iso_timestamp_to_naive_utc("2025-07-16 16:32:00"),
    # }
    # data = ProcessInfoRecord(**raw)

    # raw_update = {
    #     "tx_process_info_id": 11,
    #     "asset_guid": "update_test_asset_guid",
    #     "site_guid": "update_test_site_guid",
    #     "process_name": "update_test_process_name",
    #     "process_date": parse_iso_timestamp_to_naive_utc("2025-07-16 12:49:37"),
    #     "created_date": parse_iso_timestamp_to_naive_utc("2025-07-16 16:32:00"),
    #     "updated_date": datetime.now(timezone.utc)
    # }
    # data_update = ProcessInfoRecord(**raw_update)

    # with PgsqlActionDAO() as cur:
    #     cur.insert_new_data(data)
    #     cur.update_data(data_update)
    #     cur.commit()

    # =========================================================================

    # query_conf = {
    #     "query": get_loaders_query(),
    #     "params": [
    #         "9abcd9d9-161b-11ec-9f7a-0ae1853f1c63",
    #         "a9cb4ec9-c164-4442-9ed3-3c9a345a1117"
    #     ]
    # }

    # with PgsqlActionDAO() as pgsqlconn:
    #     data = pgsqlconn.pull_data_from_table(custom_query=query_conf)

    # if data:
    #     for d in data:
    #         print(d)
    return

    # with PgsqlActionDAO() as pgsqlconn:
    #     data = is_the_asset_cycle_truck(
    #         pgsqlconn,
    #         "6fe9d216-0928-4f3c-916c-393d2de421ee",
    #         "a9cb4ec9-c164-4442-9ed3-3c9a345a1117"
    #     )

    # print(f"status : {data}")

from models.dao.redis_source_dao import RedisSourceDAO
from typing import Optional

def get_site_regions(site_guid:str, region_type: str) -> Optional[list]:
    with RedisSourceDAO() as rd:
        all_regions = rd.get_site_regions(site_guid, region_type)

    return all_regions

if __name__ == "__main__":
    dump_regions = get_site_regions("90f2edf4-72d7-4991-a9a8-f63b5efc7afb", "dump")

    if dump_regions:
        print(dump_regions)

[
    {'region_guid': 'ce126293-706b-445d-8ab2-d84b798f02d5', 'region_name': 'Sand dump zone', 'region_points': '-71.499290 43.452649,-71.499243 43.452402,-71.498899 43.452323,-71.498783 43.452470,-71.498642 43.452811,-71.499032 43.453042,-71.499424 43.452801,-71.499290 43.452649'},
    {'region_guid': '6f9124e1-369b-4cb7-b940-7a9db2540f33', 'region_name': 'Rock dump zone', 'region_points': '-71.507342 43.449508,-71.507097 43.449315,-71.506751 43.449565,-71.507139 43.449705,-71.507429 43.449579,-71.507342 43.449508'},
    {'region_guid': 'df9f4758-45aa-4919-8789-2dd75deb8e0e', 'region_name': 'Stripping Dump Zone', 'region_points': '-71.504007 43.454765,-71.504100 43.454439,-71.503379 43.454309,-71.503262 43.454532,-71.503277 43.454743,-71.504007 43.454765'}
]
    