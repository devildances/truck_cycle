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

# =============================================================================

# from models.dao.pgsql_action_dao import PgsqlActionDAO
# from models.dto.record_dto import CycleRecord
# from datetime import datetime, timezone

# if __name__ == "__main__":
    # test_data = {
    #     "asset_guid": "TEST",
    #     "cycle_number": 1,
    #     "cycle_status": "TEST",
    #     "current_process_date": datetime.now(timezone.utc),
    #     "site_guid": "TEST",
    #     "current_segment": None,
    #     "previous_work_state_id": None,
    #     "current_work_state_id": 2,
    #     "loader_asset_guid": None,
    #     "loader_latitude": None,
    #     "loader_longitude": None,
    #     "previous_loader_distance": None,
    #     "current_loader_distance": None,
    #     "dump_region_guid": None,
    #     "is_outlier": True,
    #     "outlier_position_latitude": None,
    #     "outlier_position_longitude": None,
    #     "load_start_utc": None,
    #     "load_end_utc": None,
    #     "dump_start_utc": None,
    #     "dump_end_utc": None,
    #     "load_travel_seconds": None,
    #     "empty_travel_seconds": None,
    #     "dump_seconds": None,
    #     "load_seconds": None,
    #     "total_cycle_seconds": None,
    #     "outlier_seconds": None,
    #     "cycle_start_utc": datetime.now(timezone.utc),
    #     "cycle_end_utc": None,
    #     "created_date": datetime.now(timezone.utc),
    #     "updated_date": datetime.now(timezone.utc)
    # }
    # pull_data = {
    #     'table': 'asset_cycle_tmp_vlx_dev',
    #     'target_columns': ["cycle_number", "cycle_status", "is_outlier"],
    #     'target_filter': 'asset_cycle_vlx_id = 181'
    # }
    # with PgsqlActionDAO() as pg:
    #     # pg.insert_new_data(CycleRecord(**test_data))
    #     data = pg.pull_data_from_table(single_query=pull_data)

    # pgsql_cred = os.getenv("PGSQL_CREDENTIALS_SECRET_NAME")
    # db = "pgsql"
    # key_id = os.getenv("AWS_ACCESS_KEY_ID")
    # key = os.getenv("AWS_SECRET_ACCESS_KEY")
    # token = os.getenv("AWS_SESSION_TOKEN")
    # region = os.getenv("AWS_REGION")
    # sec = get_db_credentials_from_secrets_manager(pgsql_cred,region,db,key_id,key,token)
    # print(sec)

# from processing.preprocess import get_all_loaders_in_site
# from models.dao.pgsql_action_dao import PgsqlActionDAO
# from utils.utilities import parse_iso_timestamp_to_naive_utc
# from models.dto.record_dto import RealtimeRecord

# conn = PgsqlActionDAO()

# curr_rec = RealtimeRecord(
#     asset_guid="test",
#     site_guid="sitetest",
#     timestamp=parse_iso_timestamp_to_naive_utc("2025-08-20 18:48:00"),
#     work_state_id=1,
#     longitude=1.00,
#     latitude=-1.00
# )

# site_loaders = get_all_loaders_in_site(
#     pgsql_conn=conn,
#     service_type="reprocess",
#     target_params=["a9cb4ec9-c164-4442-9ed3-3c9a345a1117", curr_rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")]
# )

# for i in site_loaders:
#     print(i)

# conn.close_connection()


from models.dao.pgsql_action_dao import PgsqlActionDAO
from processing.preprocess import get_all_loaders_in_site

pgsql_db_conn_obj = PgsqlActionDAO()
site_loaders = get_all_loaders_in_site(
    pgsql_conn=pgsql_db_conn_obj,
    service_type='reprocess',
    target_params=[
        'a9cb4ec9-c164-4442-9ed3-3c9a345a1117',
        '2025-08-29 10:31:30'
    ]
)

pgsql_db_conn_obj.close_connection()

print(site_loaders)


