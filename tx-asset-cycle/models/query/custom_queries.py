from psycopg2 import sql

from config.postgresql_config import (DX_SCHEMA_NAME,
                                      PGSQL_ASSET_CYCLE_TABLE_NAME,
                                      PGSQL_ASSET_TABLE_NAME,
                                      PGSQL_CHECKPOINT_TABLE_NAME,
                                      PGSQL_IDLE_CYCLE_TABLE_NAME,
                                      PGSQL_REGION_TABLE_NAME)
from config.static_config import LOADER_ASSET_TYPE_GUID


def get_regions_query() -> sql.SQL:
    query_parts = []

    cte_cols = [
        "r.guid AS region_guid",
        "r.points AS region_points",
        "r.fence_area",
        "ROW_NUMBER() OVER(PARTITION by a.guid, a.client_guid, "
        "rs.guid, r.guid ORDER BY r.created_date DESC) as rn"
    ]
    cte_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in cte_cols]
    )
    cte_query = sql.SQL(
        """
        WITH cte1 AS (
            SELECT {columns}
            FROM {schema}.{asset_tbl} a
            JOIN {schema}.{ref_site_tbl} rs
            ON {ars_join}
            JOIN {schema}.{region_tbl} r
            ON {rsr_join}
            WHERE
                a.guid = %s
                AND r.points IS NOT NULL
                AND r.is_deleted = false
        )
        """
    ).format(
        columns=cte_columns_sql,
        schema=sql.Identifier(DX_SCHEMA_NAME),
        asset_tbl=sql.Identifier(PGSQL_ASSET_TABLE_NAME),
        ref_site_tbl=sql.Identifier("ref_site"),
        region_tbl=sql.Identifier(PGSQL_REGION_TABLE_NAME),
        ars_join=sql.SQL("a.site_guid = rs.guid"),
        rsr_join=sql.SQL("rs.guid = r.site_guid")
    )

    main_cols = ["region_guid", "region_points"]
    main_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in main_cols]
    )
    main_query = sql.SQL("SELECT {columns} FROM {cte}").format(
        columns=main_columns_sql,
        cte=sql.SQL("cte1")
    )

    query_parts.append(cte_query)
    query_parts.append(main_query)
    query_parts.append(sql.SQL("WHERE rn = 1"))
    query_parts.append(sql.SQL("ORDER BY fence_area ASC"))

    return sql.SQL(" ").join(query_parts) + sql.SQL(";")


def get_loaders_from_process_info_query() -> sql.SQL:
    query_parts = []

    target_cols = [
        "asset_guid",
        "site_guid",
        "current_latitude AS latitude",
        "current_longitude AS longitude"
    ]

    target_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in target_cols]
    )

    main_query = sql.SQL(
        "SELECT {columns} FROM {schema}.{pi_tbl}"
    ).format(
        columns=target_columns_sql,
        schema=sql.Identifier(DX_SCHEMA_NAME),
        pi_tbl=sql.Identifier(PGSQL_CHECKPOINT_TABLE_NAME)
    )

    query_parts.append(main_query)
    query_parts.append(
        sql.SQL("WHERE process_name = 'asset_idle_vlx'")
    )
    query_parts.append(sql.SQL("AND site_guid = %s"))
    query_parts.append(
        sql.SQL("AND asset_type_guid = {asset_type}").format(
            asset_type=sql.Literal(LOADER_ASSET_TYPE_GUID)
        )
    )
    query_parts.append(sql.SQL("AND current_latitude IS NOT NULL"))
    query_parts.append(sql.SQL("AND current_longitude IS NOT NULL"))
    return sql.SQL(" ").join(query_parts) + sql.SQL(";")


def get_loaders_from_asset_idle_query() -> sql.SQL:
    query_parts = []

    asset_ids_cte_cols = [
        "guid AS asset_guid",
        "site_guid"
    ]
    asset_ids_cte_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in asset_ids_cte_cols]
    )
    asset_ids_cte_query = sql.SQL(
        """
        WITH asset_ids AS (
            SELECT {columns}
            FROM {schema}.{asset_tbl}
            WHERE
                site_guid = %s
                AND asset_type_guid = '{type_guid}'
        ),
        """
    ).format(
        columns=asset_ids_cte_columns_sql,
        schema=sql.Identifier(DX_SCHEMA_NAME),
        asset_tbl=sql.Identifier(PGSQL_ASSET_TABLE_NAME),
        type_guid=sql.SQL(LOADER_ASSET_TYPE_GUID)
    )

    latest_loc_cte_cols = [
        "ai.asset_guid",
        "ai.site_guid",
        "v.end_location_latitude AS latitude",
        "v.end_location_longitude AS longitude"
    ]
    latest_loc_cte_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in latest_loc_cte_cols]
    )
    latest_loc_cte_query = sql.SQL(
        """
        latest_idle_locations AS (
            SELECT DISTINCT ON (ai.asset_guid) {columns}
            FROM {cte} ai
            JOIN {schema}.{idle_tbl} v
            ON {arg_join}
            WHERE
                v.end_location_latitude IS NOT NULL
                AND v.end_location_longitude IS NOT NULL
                AND COALESCE(v.end_state_utc, v.start_state_utc)
                    AT TIME ZONE 'UTC' <= %s
            ORDER BY
                ai.asset_guid,
                COALESCE(v.end_state_utc, v.start_state_utc) DESC
        )
        """
    ).format(
        columns=latest_loc_cte_columns_sql,
        cte=sql.SQL("asset_ids"),
        schema=sql.Identifier(DX_SCHEMA_NAME),
        idle_tbl=sql.Identifier(PGSQL_IDLE_CYCLE_TABLE_NAME),
        arg_join=sql.SQL("v.asset_guid = ai.asset_guid")
    )

    main_cols = [
        "asset_guid", "site_guid", "latitude", "longitude"
    ]
    main_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in main_cols]
    )
    main_query = sql.SQL("SELECT {columns} FROM {cte}").format(
        columns=main_columns_sql,
        cte=sql.SQL("latest_idle_locations")
    )

    query_parts.append(asset_ids_cte_query)
    query_parts.append(latest_loc_cte_query)
    query_parts.append(main_query)

    return sql.SQL(" ").join(query_parts) + sql.SQL(";")


def get_last_cycle_number_asset_query() -> sql.SQL:
    query_parts = []

    main_cols = ["MAX(cycle_number) AS cycle_number"]
    main_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in main_cols]
    )
    main_query = sql.SQL("SELECT {columns} FROM {cycle_table}").format(
        columns=main_columns_sql,
        cycle_table=sql.Identifier(PGSQL_ASSET_CYCLE_TABLE_NAME)
    )
    query_parts.append(main_query)
    query_parts.append(sql.SQL("WHERE asset_guid=%s"))
    query_parts.append(sql.SQL("AND cycle_end_utc AT TIME ZONE 'UTC' <= %s"))

    return sql.SQL(" ").join(query_parts) + sql.SQL(";")
