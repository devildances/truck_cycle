from psycopg2 import sql

from config.postgresql_config import DX_SCHEMA_NAME


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
        asset_tbl=sql.Identifier("asset"),
        ref_site_tbl=sql.Identifier("ref_site"),
        region_tbl=sql.Identifier("region"),
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


def get_loaders_query() -> sql.SQL:
    query_parts = []
    target_cols = [
        "a.guid AS asset_guid",
        "a.site_guid AS site_guid",
        "ai.location_latitude AS latitude",
        "ai.location_longitude AS longitude"
    ]
    filter = (
        "a.asset_type_guid = %s "
        "AND a.site_guid = %s "
        "AND ai.location_latitude IS NOT NULL "
        "AND ai.location_longitude IS NOT NULL"
    )
    target_columns_sql = sql.SQL(", ").join(
        [sql.SQL(col) for col in target_cols]
    )

    main_query = sql.SQL(
        """
        SELECT {columns}
        FROM {schema}.{left_tbl} a
        JOIN {schema}.{right_tbl} ai
        """
    ).format(
        columns=target_columns_sql,
        schema=sql.Identifier(DX_SCHEMA_NAME),
        left_tbl=sql.Identifier("asset"),
        right_tbl=sql.Identifier("asset_info")
    )
    join_on = sql.SQL("ON a.guid = ai.asset_guid")

    query_parts.append(main_query)
    query_parts.append(join_on)
    query_parts.append(sql.SQL("WHERE ") + sql.SQL(filter))

    return sql.SQL(" ").join(query_parts) + sql.SQL(";")
