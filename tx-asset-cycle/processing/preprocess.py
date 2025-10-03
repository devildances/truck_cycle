import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from shapely.geometry import Point as ShapelyPoint

from config.postgresql_config import (PGSQL_ASSET_CYCLE_PROCESS_NAME,
                                      PGSQL_ASSET_CYCLE_REPRO_TMP_TABLE_NAME,
                                      PGSQL_ASSET_CYCLE_TMP_TABLE_NAME,
                                      PGSQL_ASSET_TABLE_NAME,
                                      PGSQL_CHECKPOINT_TABLE_NAME)
from config.static_config import (DISTANCE_THRESHOLD_FOR_LOADER,
                                  TRUCK_ASSET_TYPE_GUID)
from models.dao.pgsql_action_dao import PgsqlActionDAO
from models.dao.redis_source_dao import RedisSourceDAO
from models.dto.asset_dto import LoaderAsset, TruckAsset
from models.dto.record_dto import (CycleRecord, ProcessInfoRecord,
                                   RealtimeRecord)
from models.dto.region_polygon_dto import RegionPoly
from models.query.custom_queries import (get_last_cycle_number_asset_query,
                                         get_loaders_from_asset_idle_query,
                                         get_loaders_from_process_info_query)
from models.query.target_columns import LAST_RECORD_COLS, LATEST_PI_COLS
from utils import checkpoint
from utils.utilities import (get_stack_trace_py, haversine_distance,
                             validate_distance_threshold)

logger = logging.getLogger(__name__)


def get_site_guid(
    pgsql_conn: PgsqlActionDAO,
    redis_conn: RedisSourceDAO,
    asset_guid: str
) -> Optional[str]:
    """
    Retrieve the site GUID for an asset with multi-tier caching strategy.

    Implements a three-tier lookup strategy to efficiently determine which
    site an asset belongs to:
    1. In-memory cache (6-hour TTL)
    2. Redis cache (real-time data)
    3. PostgreSQL database (fallback)

    This approach minimizes database queries for frequently accessed assets
    while ensuring data freshness through TTL-based cache invalidation.

    Cache Strategy:
        - Memory cache: Ultra-fast, 6-hour TTL for stable assignments
        - Redis: Fast, real-time updates from region associations
        - PostgreSQL: Authoritative source, used when caches miss

    Args:
        pgsql_conn: Active PostgreSQL connection for database queries.
                   Used as fallback when cache misses occur.
        redis_conn: Active Redis connection for real-time lookups.
                   Checks current region-based site assignments.
        asset_guid: Unique identifier of the asset to look up.
                   Expected format: UUID string.

    Returns:
        Optional[str]: Site GUID where the asset is currently assigned.
                      Returns None if asset not found in any data source
                      or if errors occur during lookup.

    Side Effects:
        - Updates global checkpoint.ASSETS_SITE_GUID cache on successful
          lookups
        - Cache entries include timestamp for TTL management

    Example:
        >>> pgsql_dao = PgsqlActionDAO()
        >>> redis_dao = RedisSourceDAO()
        >>> site_id = get_site_guid(pgsql_dao, redis_dao, "truck-123")
        >>> if site_id:
        ...     print(f"Asset assigned to site: {site_id}")
        ... else:
        ...     print("Asset has no site assignment")

    Performance Characteristics:
        - Memory cache hit: O(1), ~0.001ms
        - Redis hit: O(n) where n = regions, ~5-10ms
        - PostgreSQL hit: O(1) with index, ~20-50ms
        - Cache miss penalty decreases with repeated lookups

    Error Handling:
        - Redis failures fall back to PostgreSQL
        - PostgreSQL failures return None (logged)
        - Corrupted cache entries are refreshed
    """
    # Check in-memory cache with TTL validation
    if checkpoint.ASSETS_SITE_GUID and asset_guid in checkpoint.ASSETS_SITE_GUID:
        cache_entry = checkpoint.ASSETS_SITE_GUID[asset_guid]

        # Calculate cache age in hours
        cache_age = datetime.now(timezone.utc) - cache_entry["created_at"]
        cache_age_hours = cache_age.total_seconds() / 3600

        if cache_age_hours <= 6:
            return cache_entry["site_guid"]
        else:
            del checkpoint.ASSETS_SITE_GUID[asset_guid]

    try:
        # Try Redis first (real-time data)
        site_guid = redis_conn.get_asset_site(asset_guid)

        if site_guid:
            # Update cache with Redis result
            checkpoint.ASSETS_SITE_GUID[asset_guid] = {
                "site_guid": site_guid,
                "created_at": datetime.now(timezone.utc)
            }
            return site_guid

        query_params = {
            "table": PGSQL_ASSET_TABLE_NAME,
            "target_columns": ["site_guid"],
            "target_filter": f"guid = '{asset_guid}'",
            "target_limit": 1
        }

        result = pgsql_conn.pull_data_from_table(
            single_query=query_params
        )

        if result and result[0].get("site_guid"):
            site_guid = result[0]["site_guid"]

            # Update cache with PostgreSQL result
            checkpoint.ASSETS_SITE_GUID[asset_guid] = {
                "site_guid": site_guid,
                "created_at": datetime.now(timezone.utc)
            }

            return site_guid
        else:
            return None

    except Exception as error:
        logger.error(
            f"Error retrieving site_guid for asset {asset_guid}: "
            f"{type(error).__name__}: {error}",
            exc_info=True
        )
        return None


def is_the_asset_cycle_truck(
    pgsql_conn: PgsqlActionDAO,
    asset_guid: str,
    site_guid: str
) -> bool:
    """Determine if the specified asset is a truck eligible for cycle tracking.

    Queries the asset table to verify that the given asset GUID corresponds
    to a truck asset type within the specified site. This validation is
    essential for ensuring only appropriate vehicles are processed in the
    cycle tracking system.

    Args:
        pgsql_conn (PgsqlActionDAO): Active PostgreSQL connection for
            database queries.
        asset_guid (str): Unique identifier of the asset to validate.
        site_guid (str): Unique identifier of the site where the asset
            is located.

    Returns:
        bool: True if the asset is a truck (matches TRUCK_ASSET_TYPE_GUID),
            False otherwise or if asset not found.

    Example:
        >>> dao = PgsqlActionDAO()
        >>> is_truck = is_the_asset_cycle_truck(
        ...     dao, "truck-123", "site-456"
        ... )
        >>> print(is_truck)  # True if truck, False otherwise
    """
    # Check in-memory cache with TTL validation
    if checkpoint.ASSETS_TYPE and asset_guid in checkpoint.ASSETS_TYPE:
        cache_entry = checkpoint.ASSETS_TYPE[asset_guid]

        # Calculate cache age in hours
        cache_age = datetime.now(timezone.utc) - cache_entry["created_at"]
        cache_age_hours = cache_age.total_seconds() / 3600

        if cache_age_hours <= 12:
            if cache_entry["asset_type_guid"] == TRUCK_ASSET_TYPE_GUID:
                return True
            return False
        else:
            del checkpoint.ASSETS_TYPE[asset_guid]

    try:
        check_asset_type_params = {
            "table": PGSQL_ASSET_TABLE_NAME,
            "target_columns": [
                "guid AS asset_guid",
                "site_guid",
                "asset_type_guid"
            ],
            "target_filter": (
                f"guid = '{asset_guid}' "
                f"AND site_guid = '{site_guid}' "
            ),
            "target_limit": 1
        }
        asset_rec = pgsql_conn.pull_data_from_table(
            single_query=check_asset_type_params
        )
        if asset_rec:
            tmp_asset = TruckAsset(**asset_rec[0])

            # Update cache with PostgreSQL result
            checkpoint.ASSETS_TYPE[asset_guid] = {
                "asset_type_guid": tmp_asset.asset_type_guid,
                "created_at": datetime.now(timezone.utc)
            }

            if tmp_asset.asset_type_guid == TRUCK_ASSET_TYPE_GUID:
                return True
        return False
    except Exception as e:
        logger.error(
            f"Error retrieving asset_type for asset {asset_guid}: {e}"
        )
        return False


def is_current_record_older(
    pgsql_conn: PgsqlActionDAO,
    current_record: RealtimeRecord,
) -> bool:
    """Check if current record is older than the last processed timestamp.

    Args:
        pgsql_conn: PostgreSQL connection object for database queries
        current_record: Current realtime record to validate

    Returns:
        True if current record is older than or equal to last processed
        timestamp, False otherwise

    Notes:
        - First checks in-memory cache for performance
        - Falls back to database query if cache miss
        - Minimizes object creation and string operations for GC efficiency
    """
    asset_guid = current_record.asset_guid
    current_timestamp = current_record.timestamp

    if checkpoint.LATEST_PROCESS_INFO and asset_guid in checkpoint.LATEST_PROCESS_INFO:
        try:
            cached_last_timestamp = checkpoint.LATEST_PROCESS_INFO[asset_guid]

            if current_timestamp <= cached_last_timestamp:
                logger.warning(
                    f"[PREPROCESS WARNING] {current_record.asset_guid} "
                    f"{current_record.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Current data is older or equal than the last data point "
                    f"({cached_last_timestamp.strftime('%Y-%m-%d %H:%M:%S')}) "
                    "in in-memory cache."
                )
                return True
        except Exception as parse_error:
            logger.error(
                f"Error parsing cached timestamp for asset {asset_guid}: "
                f"{parse_error}"
            )
        else:
            return False

    try:
        filter_statement = (
            f"asset_guid = '{asset_guid}' "
            f"AND process_name = '{PGSQL_ASSET_CYCLE_PROCESS_NAME}' "
            f"AND process_date >= '{current_timestamp}'"
        )
        latest_pi_params = {
            "table": PGSQL_CHECKPOINT_TABLE_NAME,
            "target_columns": ["tx_process_info_id", "process_date"],
            "target_filter": filter_statement,
            "target_limit": 1,
        }
        latest_pi = pgsql_conn.pull_data_from_table(single_query=latest_pi_params)
        if latest_pi:
            logger.warning(
                f"[PREPROCESS WARNING] {current_record.asset_guid} "
                f"{current_record.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                "Current data is older or equal than the last data point "
                f"in the process info table."
            )
            return True
    except Exception as db_error:
        logger.error(
            f"Error querying database for asset {asset_guid}: {db_error}"
        )
        return False

    return False


def get_latest_process_info(
    pgsql_conn: PgsqlActionDAO,
    asset_guid: str,
    site_guid: str,
    current_record_timestamp: datetime
) -> ProcessInfoRecord:
    """Retrieve or create process info record for the current processing run.

    Queries the checkpoint table for the most recent process info record
    for the specified asset. If an existing record is found, creates an
    updated version with the current timestamp. If no record exists,
    initializes a new process info record with the provided parameters.

    Args:
        pgsql_conn (PgsqlActionDAO): Active PostgreSQL connection for
            database queries.
        asset_guid (str): Unique identifier of the asset being processed.
        site_guid (str): Unique identifier of the site where the asset
            is located.
        current_record_timestamp (datetime): Timestamp of the current
            record being processed.

    Returns:
        ProcessInfoRecord: Process info record with either updated existing
            data or newly initialized values for tracking processing state.

    Note:
        The returned record includes created_date from existing records or
        current UTC time for new records, and always sets updated_date to
        current UTC time for existing records.
    """
    pi_record = None
    latest_pi_params = {
        "table": PGSQL_CHECKPOINT_TABLE_NAME,
        "target_columns": LATEST_PI_COLS,
        "target_filter": (
            f"asset_guid = '{asset_guid}' "
            f"AND process_name = '{PGSQL_ASSET_CYCLE_PROCESS_NAME}' "
        ),
        "target_limit": 1,
        "target_order": "process_date DESC"
    }
    latest_process_info = pgsql_conn.pull_data_from_table(
        single_query=latest_pi_params
    )
    if latest_process_info:
        lpi = latest_process_info[0]
        pi_record = {
            "tx_process_info_id": lpi.get("tx_process_info_id"),
            "asset_guid": lpi.get("asset_guid"),
            "site_guid": lpi.get("site_guid"),
            "process_name": lpi.get("process_name"),
            "process_date": current_record_timestamp,
            "created_date": lpi.get("created_date"),
            "updated_date": datetime.now(timezone.utc)
        }
    else:
        pi_record = {
            "asset_guid": asset_guid,
            "site_guid": site_guid,
            "process_name": PGSQL_ASSET_CYCLE_PROCESS_NAME,
            "process_date": current_record_timestamp,
            "created_date": datetime.now(timezone.utc),
            "updated_date": None
        }
    return ProcessInfoRecord(**pi_record)


def get_last_cycle_details(
    pgsql_conn: PgsqlActionDAO,
    site_guid: str,
    asset_guid: str,
    service_type: str
) -> Optional[CycleRecord]:
    """Retrieve the most recent in-progress cycle record for an asset.

    Queries the asset cycle table to find the latest cycle with
    'INPROGRESS' status for the specified asset and site. This is used
    to determine if there's an ongoing cycle that needs to be continued
    or completed.

    Args:
        pgsql_conn (PgsqlActionDAO): Active PostgreSQL connection for
            database queries.
        site_guid (str): Unique identifier of the site where the asset
            is located.
        asset_guid (str): Unique identifier of the asset to query.

    Returns:
        Optional[CycleRecord]: The most recent in-progress cycle record,
            or None if no in-progress cycles are found for the asset.

    Note:
        Results are ordered by cycle_number DESC to ensure the most
        recent cycle is returned when multiple in-progress cycles exist
        (which shouldn't happen under normal circumstances).
    """
    if service_type == "realtime":
        target_table = PGSQL_ASSET_CYCLE_TMP_TABLE_NAME
    else:
        target_table = PGSQL_ASSET_CYCLE_REPRO_TMP_TABLE_NAME

    last_cycle_params = {
        "table": target_table,
        "target_columns": LAST_RECORD_COLS,
        "target_filter": (
            f"asset_guid = '{asset_guid}' "
            f"AND site_guid = '{site_guid}' "
            "AND cycle_status = 'INPROGRESS'"
        ),
        "target_limit": 1,
        "target_order": "cycle_number DESC"
    }
    last_cycle_rec = pgsql_conn.pull_data_from_table(
        single_query=last_cycle_params
    )
    return CycleRecord(**last_cycle_rec[0]) if last_cycle_rec else None


def get_next_cycle_number_for_reprocess(
        pgsql_conn: PgsqlActionDAO,
        target_params: Optional[list]
) -> Optional[int]:
    query_config = {
        "query": get_last_cycle_number_asset_query(),
        "params": target_params if target_params else None
    }
    try:
        record = pgsql_conn.pull_data_from_table(
            custom_query=query_config
        )
        next_cycle_number = record[0].get("cycle_number")
        if next_cycle_number:
            next_cycle_number = int(next_cycle_number) + 1
        return next_cycle_number
    except Exception as e:
        logger.error(
            "Error retrieving last cycle number "
            f"for asset {target_params[0]}: {e}"
        )


def get_all_loaders_in_site(
    pgsql_conn: PgsqlActionDAO,
    service_type: str,
    target_params: Optional[list]
) -> List[LoaderAsset]:
    """Retrieve all loader assets in a site using a custom SQL query.

    Executes a custom SQL query to fetch loader assets and converts the
    results into a list of LoaderAsset objects. This function provides
    flexibility for complex loader queries that may include joins,
    filtering, or specific site conditions.

    Args:
        pgsql_conn (PgsqlActionDAO): Active PostgreSQL connection for
            database queries.
        target_query (sql.SQL): Prepared SQL query object for fetching
            loader data.
        target_params (Optional[list]): Parameters for the SQL query,
            or None if no parameters are needed.

    Returns:
        List[LoaderAsset]: List of LoaderAsset objects representing all
            loaders found by the query. Returns empty list if no loaders
            are found.

    Example:
        >>> query = sql.SQL("SELECT * FROM loaders WHERE site_guid = %s")
        >>> loaders = get_all_loaders_in_site(dao, query, ["site-123"])
        >>> print(len(loaders))  # Number of loaders found
    """
    site_guid = target_params[0]

    if service_type == "realtime":
        target_query = get_loaders_from_process_info_query()
        query_params = target_params[:1]
    else:
        target_query = get_loaders_from_asset_idle_query()
        query_params = target_params

    try:
        target_loaders = []
        all_loaders_params = {
            "query": target_query,
            "params": query_params if query_params else None
        }
        loaders = pgsql_conn.pull_data_from_table(
            custom_query=all_loaders_params
        )
        if loaders:
            for loader in loaders:
                target_loaders.append(LoaderAsset(**loader))
        return target_loaders
    except Exception as e:
        logger.error(
            f"Error retrieving site loaders for site {site_guid}: {e}"
        )
        return []


def is_truck_near_any_loader(
    truck_location: List[float],
    list_of_loaders: List[LoaderAsset]
) -> Optional[Tuple[LoaderAsset, float]]:
    """Find the first loader within distance threshold of truck location.

    Calculates the haversine distance between the truck's current location
    and each loader in the provided list. Returns the first loader found
    within the configured distance threshold along with the calculated
    distance.

    Args:
        truck_location (List[float]): Truck coordinates as [latitude,
            longitude].
        list_of_loaders (List[LoaderAsset]): List of loader assets to
            check against.

    Returns:
        Optional[Tuple[LoaderAsset, float]]: Tuple containing the first
            loader within threshold and its distance in meters, or
            (None, None) if no loader is within range.

    Raises:
        ValueError: If coordinates or distance threshold are invalid.
        TypeError: If inputs are not properly formatted.

    Note:
        Uses DISTANCE_THRESHOLD_FOR_LOADER from static configuration.
        Returns on first match for performance (early exit).

    Example:
        >>> truck_pos = [40.7128, -74.0060]  # NYC coordinates
        >>> loader, distance = is_truck_near_any_loader(truck_pos, loaders)
        >>> if loader:
        ...     print(f"Nearest loader: {distance:.2f}m away")
    """
    # Validate distance threshold
    validate_distance_threshold(DISTANCE_THRESHOLD_FOR_LOADER)
    tmp = []

    for loader in list_of_loaders:
        distance = haversine_distance(
            point1=truck_location,
            point2=[loader.latitude, loader.longitude]
        )
        if distance <= DISTANCE_THRESHOLD_FOR_LOADER:
            tmp.append((loader, distance,))

    if tmp:
        return min(tmp, key=lambda x: x[1])
    else:
        return (None, None,)


def get_all_regions(
    redis_conn: RedisSourceDAO,
    site_guid: str,
    region_type: str
) -> Optional[List[RegionPoly]]:
    """
    Retrieve all regions for a mining site as polygon objects.

    Fetches region data from Redis and converts them into RegionPoly
    objects suitable for spatial operations. Each region includes geographic
    boundaries and metadata needed for point-in-polygon testing and other
    spatial calculations in mining operations.

    This function is typically used to:
    - Load all zones for geofencing calculations
    - Determine if trucks are within authorized areas
    - Calculate distances to nearest regions
    - Validate operations against defined boundaries

    Args:
        redis_conn: Active Redis data access object for retrieving region
                   data. Must have an established connection.
        site_guid: Unique identifier for the mining site. Expected format
                  is UUID string (e.g., "90f2edf4-72d7-4991-a9a8-f63b5efc7afb").

    Returns:
        Optional[List[RegionPoly]]: List of RegionPoly objects representing
                                   all regions for the specified site.
                                   Returns None if no regions exist or
                                   if Redis operations fail.

        Each RegionPoly contains:
        - region_guid: Unique identifier for the region
        - name: Human-readable name of the area
        - region_points: Coordinate string for polygon boundary
        - dump_site: True or False (these are dump regions)
        - load_site: True or False (these are load regions)

    Raises:
        RedisConnectionError: If Redis operations fail during data retrieval.
                             This is propagated from the Redis DAO layer.

    Example:
        >>> from models.dao.redis_source_dao import RedisSourceDAO
        >>>
        >>> # Get all regions for a site
        >>> redis_dao = RedisSourceDAO()
        >>> site_id = "90f2edf4-72d7-4991-a9a8-f63b5efc7afb"
        >>> region_type = "load"
        >>> load_regions = get_all_regions(redis_dao, site_id, region_type)
        >>>
        >>> if load_regions:
        ...     print(f"Found {len(load_regions)} regions")
        ...     for region in load_regions:
        ...         polygon = region.get_polygon()
        ...         if polygon and polygon.is_valid:
        ...             print(f"Region {region.name}: "
        ...                   f"Area = {polygon.area} sq units")
        ... else:
        ...     print(f"No {region_type} regions found")

    Performance Considerations:
        - Redis query is performed once for all regions
        - Polygon parsing is deferred until get_polygon() is called
        - Consider caching results if called frequently with same site

    Note:
        - Only regions with valid region_guid are included
        - Malformed regions are skipped with warning logs
        - Empty region_points are allowed (polygon parsing fails later)
        - Site must exist in Redis or None is returned
    """
    if region_type not in ["dump", "load"]:
        logger.error(
            f"get_all_regions() - region_type of {region_type} is unknown!"
        )
        return None
    try:
        # Retrieve dump regions from Redis
        raw_regions = redis_conn.get_site_regions(site_guid, region_type)

        if not raw_regions:
            return None

        # Convert to RegionPoly objects
        regions = []

        for region_data in raw_regions:
            # Validate required fields
            region_guid = region_data.get("region_guid")
            if not region_guid:
                logger.warning(
                    "Skipping region with missing region_guid: %s",
                    region_data
                )
                continue

            # Create RegionPoly with region-specific settings
            region_poly = RegionPoly(
                region_guid=region_guid,
                site_guid=site_guid,
                name=region_data.get("region_name"),
                region_points=region_data.get("region_points"),
                region_type=region_type,
                load_site=True if region_type == "load" else False,
                dump_site=True if region_type == "dump" else False
            )

            regions.append(region_poly)

        return regions if regions else None

    except Exception as e:
        logger.error(
            "Unexpected error loading %s regions for site %s: %s",
            region_type,
            site_guid,
            str(e)
        )

        return None


def is_asset_within_dump_region(
    asset_type: str,
    asset_location: List[float],
    list_of_dump_regions: List[RegionPoly]
) -> Optional[RegionPoly]:
    """Check if truck location intersects with any dump region polygon.

    Args:
        truck_location: List containing [latitude, longitude] coordinates
        list_of_dump_regions: List of RegionPoly objects to check intersection

    Returns:
        First RegionPoly that intersects with truck location, or None if no
        intersection found or validation fails

    Notes:
        - Uses shapely Point and polygon intersection for accurate calculation
        - Validates coordinates before processing
        - Returns first matching region (early exit for performance)
        - Minimizes object creation to reduce garbage collection overhead
    """
    # Early validation
    if not list_of_dump_regions:
        logger.debug("is_asset_within_dump_region: region_list is empty or None.")
        return None

    latitude = asset_location[0]
    longitude = asset_location[1]

    if latitude is None or longitude is None:
        logger.debug("is_asset_within_dump_region: latitude or longitude is None.")
        return None

    if latitude == longitude:
        logger.debug(
            f"is_asset_within_dump_region: Latitude and longitude are equal "
            f"({latitude}), skipping calculation process."
        )
        return None

    try:
        asset_point = ShapelyPoint(longitude, latitude)
    except Exception as point_error:
        logger.error(
            f"Error creating ShapelyPoint for coordinates "
            f"Lat: {latitude}, Lon: {longitude} - Error: {point_error}\n"
            f"Trace: {get_stack_trace_py(point_error)}"
        )
        return None

    for dump_region in list_of_dump_regions:
        if dump_region is None:
            continue

        region_poly = dump_region.get_polygon()
        if region_poly is None:
            error_msg = getattr(dump_region, "_parse_error_message", "Unknown")
            logger.error(
                f"Skipping Region GUID {dump_region.region_guid} for point "
                f"({longitude},{latitude}) as its polygon could not be "
                f"obtained. Reason: {error_msg}"
            )
            continue

        try:
            if region_poly.intersects(asset_point):
                logger.debug(
                    f"{asset_type} within this {dump_region.region_guid} "
                    f"dump region."
                )
                return dump_region
        except Exception as intersect_error:
            logger.error(
                f"Error in is_asset_within_dump_region during intersect check "
                f"for Region GUID: {dump_region.region_guid}, "
                f"SiteGuid: {dump_region.site_guid}, "
                f"Lat: {latitude}, Lon: {longitude} - "
                f"Error: {intersect_error}\n"
                f"Trace: {get_stack_trace_py(intersect_error)}"
            )
    return None


def get_nearest_load_region(
    asset_location: List[float],
    list_of_load_regions: List[RegionPoly]
) -> Tuple[Optional[RegionPoly], bool, Optional[float]]:
    """
    Find the nearest load region to an asset and determine containment status.

    Analyzes the spatial relationship between an asset's current position and
    all available load regions within a mining site. The function performs two
    key operations: checks if the asset is within any load region boundary,
    and calculates the distance to the nearest load region when the asset is
    outside all regions.

    This function is essential for:
    - Optimizing truck routing to the nearest available loading zone
    - Detecting when assets enter load regions for cycle state transitions
    - Calculating travel distances for productivity metrics
    - Supporting geofencing alerts when approaching load zones

    Algorithm Overview:
        1. Validates input parameters and coordinates
        2. Creates a Shapely Point from asset coordinates
        3. Checks intersection with all load region polygons
        4. If outside all regions, calculates distance to each polygon
        5. Returns the nearest region with containment and distance info

    Args:
        asset_location: Geographic coordinates as [latitude, longitude] in
                       decimal degrees. Must be valid WGS84 coordinates with
                       latitude in range [-90, 90] and longitude in range
                       [-180, 180].
        list_of_load_regions: Collection of RegionPoly objects representing
                            all load zones in the current site. Each region
                            must have valid polygon geometry for spatial
                            calculations.

    Returns:
        Tuple[Optional[RegionPoly], bool, Optional[float]]: A tuple containing
            three elements:

            1. RegionPoly or None: The load region that either contains the
               asset or is nearest to it. Returns None only if no valid
               regions are provided or coordinate parsing fails.

            2. bool: True if the asset_location is within any load region
               boundary (point-in-polygon test passes), False if the asset
               is outside all load regions.

            3. float or None: Distance in meters from the asset to the nearest
               load region boundary. Returns None when is_in_load_region is
               True (distance is effectively zero when inside). Returns the
               minimum distance to the nearest polygon edge when outside all
               regions.

    Raises:
        No exceptions are raised directly. All errors are logged and the
        function returns (None, False, None) for invalid inputs.

    Example:
        >>> # Asset inside a load region
        >>> asset_pos = [40.7128, -74.0060]  # NYC coordinates
        >>> regions = get_all_regions(redis_conn, site_id, "load")
        >>> nearest, is_inside, distance = get_nearest_load_region(
        ...     asset_pos, regions
        ... )
        >>> if nearest and is_inside:
        ...     print(f"Asset in load region: {nearest.name}")
        ... elif nearest:
        ...     print(f"Nearest load region: {nearest.name}, "
        ...           f"Distance: {distance:.2f}m")

        >>> # Asset outside all load regions
        >>> asset_pos = [40.7589, -73.9851]  # Times Square
        >>> nearest, is_inside, distance = get_nearest_load_region(
        ...     asset_pos, regions
        ... )
        >>> if nearest and not is_inside:
        ...     print(f"Travel {distance:.2f}m to {nearest.name}")

    Performance Considerations:
        - Early return when asset is found within a region (O(n) worst case)
        - Distance calculations only performed when outside all regions
        - Polygon caching in RegionPoly reduces repeated parsing overhead
        - For sites with many regions (>50), consider spatial indexing

    Geometric Calculations:
        - Point-in-polygon: Uses Shapely's intersects() method which handles
          edge cases like points on boundaries
        - Distance: Calculated using Shapely's distance() method which finds
          the minimum distance from point to polygon boundary
        - Conversion: Shapely distance is in degrees, converted to meters
          using Haversine approximation (111,320 meters per degree latitude)

    Note:
        - Coordinates must be in longitude/latitude order for Shapely Point
        - Invalid polygons are skipped with warning logs
        - Distance calculation assumes flat earth approximation for small
          regions (acceptable for mining site scales)
        - Returns the first containing region found, not necessarily the
          one with the smallest area or best fit
    """
    if not list_of_load_regions:
        logger.debug(
            "get_nearest_load_region: region_list is empty or None."
        )
        return None, False, None

    if not asset_location or len(asset_location) < 2:
        logger.debug(
            "get_nearest_load_region: asset_location is invalid."
        )
        return None, False, None

    latitude = asset_location[0]
    longitude = asset_location[1]

    if latitude is None or longitude is None:
        logger.debug(
            "get_nearest_load_region: latitude or longitude is None."
        )
        return None, False, None

    if latitude == longitude:
        logger.debug(
            f"get_nearest_load_region: Latitude and longitude are equal "
            f"({latitude}), skipping calculation process."
        )
        return None, False, None

    try:
        asset_point = ShapelyPoint(longitude, latitude)
    except Exception as point_error:
        logger.error(
            f"Error creating ShapelyPoint for coordinates "
            f"Lat: {latitude}, Lon: {longitude} - Error: {point_error}\n"
            f"Trace: {get_stack_trace_py(point_error)}"
        )
        return None, False, None

    # First pass: Check if asset is within any load region
    for load_region in list_of_load_regions:
        if load_region is None:
            continue

        region_poly = load_region.get_polygon()
        if region_poly is None:
            error_msg = getattr(
                load_region, "_parse_error_message", "Unknown"
            )
            logger.error(
                f"Skipping Region GUID {load_region.region_guid} for point "
                f"({longitude},{latitude}) as its polygon could not be "
                f"obtained. Reason: {error_msg}"
            )
            continue

        try:
            if region_poly.intersects(asset_point):
                logger.debug(
                    f"Asset within load region {load_region.region_guid}"
                )
                return load_region, True, None
        except Exception as intersect_error:
            logger.error(
                f"Error in get_nearest_load_region during intersect check "
                f"for Region GUID: {load_region.region_guid}, "
                f"SiteGuid: {load_region.site_guid}, "
                f"Lat: {latitude}, Lon: {longitude} - "
                f"Error: {intersect_error}\n"
                f"Trace: {get_stack_trace_py(intersect_error)}"
            )

    # Second pass: Asset is outside all regions, find nearest one
    nearest_region = None
    min_distance = float('inf')

    for load_region in list_of_load_regions:
        if load_region is None:
            continue

        region_poly = load_region.get_polygon()
        if region_poly is None:
            continue  # Already logged in first pass

        try:
            # Calculate distance from point to polygon boundary
            distance_degrees = asset_point.distance(region_poly)

            # Convert degrees to meters (approximate)
            # 1 degree latitude ≈ 111,320 meters
            # For longitude, it varies by latitude, but we'll use a
            # simplified conversion for mining site scales
            distance_meters = distance_degrees * 111320

            if distance_meters < min_distance:
                min_distance = distance_meters
                nearest_region = load_region

        except Exception as distance_error:
            logger.error(
                f"Error calculating distance to Region GUID: "
                f"{load_region.region_guid}, SiteGuid: {load_region.site_guid}, "
                f"Lat: {latitude}, Lon: {longitude} - "
                f"Error: {distance_error}\n"
                f"Trace: {get_stack_trace_py(distance_error)}"
            )

    if nearest_region:
        logger.debug(
            f"Nearest load region: {nearest_region.region_guid}, "
            f"Distance: {min_distance:.2f} meters"
        )
        return nearest_region, False, min_distance

    # No valid regions found
    logger.warning(
        "get_nearest_load_region: No valid load regions could be processed."
    )
    return None, False, None


def record_initialization(
    current_record: RealtimeRecord,
    start_cycle_number: Optional[int]
) -> CycleRecord:
    """Initialize a new CycleRecord with default values and provided data.

    Creates a new cycle record with default values for all fields and
    sets the basic asset and site information from the current record.
    This serves as the starting point for tracking a new asset cycle,
    with all timing and location fields initialized to None and status
    set to 'INPROGRESS'.

    Args:
        current_record (RealtimeRecord): Current realtime record containing
            asset and site identifiers and current work state.

    Returns:
        CycleRecord: New cycle record instance with initialized default
            values, cycle_number set to 1, and status 'INPROGRESS'.

    Note:
        All duration fields (load_seconds, dump_seconds, etc.) are
        initialized to None and will be calculated as the cycle progresses.
        Created and updated dates are set to current UTC time.

    Example:
        >>> realtime_rec = RealtimeRecord(asset_guid="truck-123", ...)
        >>> cycle_rec = record_initialization(realtime_rec)
        >>> print(cycle_rec.cycle_status)  # 'INPROGRESS'
        >>> print(cycle_rec.cycle_number)  # 1
    """
    init_record_const = {
        "asset_guid": current_record.asset_guid,
        "cycle_number": start_cycle_number if start_cycle_number else 1,
        "cycle_status": "INPROGRESS",
        "current_process_date": current_record.timestamp,
        "current_area": current_record.current_area,
        "current_asset_longitude": current_record.longitude,
        "current_asset_latitude": current_record.latitude,
        "site_guid": current_record.site_guid,
        "current_segment": None,
        "previous_work_state_id": None,
        "current_work_state_id": current_record.work_state_id,
        "loader_asset_guid": None,
        "loader_latitude": None,
        "loader_longitude": None,
        "load_region_guid": None,
        "is_within_load_region": False,
        "asset_load_region_distance": None,
        "previous_loader_distance": None,
        "current_loader_distance": None,
        "idle_in_dump_region_guid": None,
        "tmp_idle_near_loader": None,
        "dump_region_guid": None,
        "outlier_type_id": None,
        "all_assets_in_same_dump_area": False,
        "outlier_date_utc": None,
        "outlier_loader_guid": None,
        "outlier_loader_latitude": None,
        "outlier_loader_longitude": None,
        "outlier_dump_region_guid": None,
        "is_outlier": False,
        "outlier_position_latitude": None,
        "outlier_position_longitude": None,
        "load_start_utc": None,
        "load_end_utc": None,
        "dump_start_utc": None,
        "dump_end_utc": None,
        "load_travel_seconds": None,
        "empty_travel_seconds": None,
        "dump_seconds": None,
        "load_seconds": None,
        "total_cycle_seconds": None,
        "outlier_seconds": None,
        "cycle_start_utc": current_record.timestamp,
        "cycle_end_utc": None,
        "created_date": datetime.now(timezone.utc),
        "updated_date": datetime.now(timezone.utc)
    }

    return CycleRecord(**init_record_const)
