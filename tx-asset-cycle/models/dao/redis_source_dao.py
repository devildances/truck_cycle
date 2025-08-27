import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Self

from .base_redis_dao import RedisBaseDAO, RedisConnectionError

logger = logging.getLogger(__name__)

# Constants
MAX_RETRIES = 3
DEFAULT_ENCODING = 'utf-8'


class DataAccessError(Exception):
    def __init__(
        self: Self,
        message: str,
        original_exception: Exception | None = None,
        connection_details: str | None = None
    ) -> None:
        super().__init__(message)
        self.original_exception = original_exception
        self.connection_details = connection_details

    def __str__(self: Self) -> str:
        base_message = super().__str__()
        if self.connection_details:
            base_message += f" (Connection Details: {self.connection_details})"
        return base_message


@dataclass
class RegionData:
    """
    Data structure for region information.

    This dataclass represents a region with all its associated metadata,
    providing type safety and clear structure for region data.

    Attributes:
        region_guid: Unique identifier for the region
        region_points: Geographic points defining the region
        created_date: When the region was created
        client_guid: Optional client identifier associated with the region
        site_guid: Optional site identifier associated with the region
    """
    region_guid: str
    region_points: str
    created_date: str
    client_guid: Optional[str] = None
    site_guid: Optional[str] = None

    def to_dict(self: Self) -> Dict[str, Any]:
        """
        Convert the region data to a dictionary.

        Returns:
            Dictionary representation of the region data
        """
        return {
            "region_guid": self.region_guid,
            "region_points": self.region_points,
            "created_date": self.created_date,
            "client_guid": self.client_guid,
            "site_guid": self.site_guid
        }


class RedisSourceDAO(RedisBaseDAO):
    """
    Data Access Object for Redis operations.

    This class provides a high-level interface for accessing Redis data
    with built-in retry logic, error handling, and flexible key patterns.
    It supports various Redis data structures and operations.

    Attributes:
        _max_retries: Maximum number of retry attempts for failed operations
    """

    def __init__(self: Self) -> None:
        """
        Initialize the Redis Source DAO.

        Sets up retry configuration and inherits connection management
        from the base Redis DAO.
        """
        super().__init__()
        self._max_retries = MAX_RETRIES

    def _execute_with_retry(
        self: Self,
        operation_func: Any,
        operation_description: str,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Execute a Redis operation with retry logic.

        This method attempts to execute a Redis operation up to the configured
        maximum number of retries, handling transient connection errors.

        Args:
            operation_func: The Redis operation function to execute
            operation_description: Human-readable description for logging
            *args: Arguments to pass to the operation function
            **kwargs: Keyword arguments to pass to the operation function

        Returns:
            Result of the Redis operation

        Raises:
            RedisConnectionError: If all retry attempts fail
        """
        last_exception = None

        for retry_attempt in range(self._max_retries):
            try:
                conn = self._get_connection()
                return operation_func(conn, *args, **kwargs)
            except Exception as e:
                last_exception = e
                logger.error(
                    "ERROR (RedisSourceDAO): Failed to execute "
                    f"{operation_description} "
                    f"(attempt {retry_attempt + 1}/{self._max_retries}): {e}"
                )

        # All retries exhausted
        raise RedisConnectionError(
            f"Failed to execute {operation_description} after "
            f"{self._max_retries} attempts\n{last_exception}",
            last_exception
        ) from last_exception

    def _get_hash_data(self: Self, key: str) -> Dict[str, str]:
        """
        Get all fields and values from a Redis hash.

        Args:
            key: Redis key for the hash

        Returns:
            Dictionary containing all hash fields and values
        """
        def operation(conn: Any, hash_key: str) -> Dict[str, str]:
            hash_data = conn.hgetall(hash_key)
            return {
                k.decode(DEFAULT_ENCODING)
                if isinstance(k, bytes)
                else str(k): v.decode(DEFAULT_ENCODING)
                if isinstance(v, bytes)
                else str(v)
                for k, v in hash_data.items()
            }

        return self._execute_with_retry(
            operation, f"get hash data for key '{key}'", key
        )

    def get_asset_site(
        self: Self,
        asset_guid: str
    ) -> Optional[str]:
        """
        Retrieve the site identifier for a given asset based on its regions.

        Determines which site an asset belongs to by examining all regions
        associated with the asset and finding the most recently created region
        that has a site assignment. This is useful for assets that may move
        between sites or have historical region associations.

        The method performs the following operations:
        1. Retrieves all region GUIDs associated with the asset
        2. Fetches region data for each region from Redis
        3. Filters regions that have site_guid assignments
        4. Returns the site_guid from the most recently created region

        Args:
            asset_guid: Unique identifier for the asset whose site assignment
                    is being queried. Expected format is a UUID string
                    (e.g., "550e8400-e29b-41d4-a716-446655440000").

        Returns:
            Optional[str]: The site GUID where the asset is currently assigned,
                        based on the most recent region association. Returns
                        None if the asset has no regions or no regions with
                        site assignments.

        Raises:
            RedisConnectionError: If Redis operations fail during data retrieval.
                                This is propagated from the underlying Redis
                                operations.

        Example:
            >>> dao = RedisSourceDAO()
            >>> asset_id = "truck-123-456"
            >>> site_id = dao.get_asset_site(asset_id)
            >>> if site_id:
            ...     print(f"Asset {asset_id} belongs to site {site_id}")
            ... else:
            ...     print(f"Asset {asset_id} has no site assignment")

        Algorithm Details:
            When multiple regions with different sites exist, the function
            selects the site from the region with the latest created_date.
            This ensures that if an asset moves between sites, the most
            recent assignment takes precedence.

        Performance Considerations:
            - Makes 1 + N Redis calls where N is the number of regions
            - Consider caching if called frequently for the same asset
            - For bulk operations, consider a batch retrieval approach

        Note:
            - Assets without any regions return None
            - Regions without site_guid are ignored
            - Only the most recent region per site is considered
            - Created dates are compared as strings (ISO format expected)
        """
        try:
            # Get all region GUIDs for this asset
            asset_regions_key = f"asset:{asset_guid}:regions"

            def get_region_guids(conn: Any) -> set:
                return conn.smembers(asset_regions_key)

            region_guids = self._execute_with_retry(
                get_region_guids,
                f"get region GUIDs for asset '{asset_guid}'"
            )

            if not region_guids:
                logger.debug(
                    f"No regions found for asset {asset_guid}"
                )
                return None

            # Collect regions with site assignments
            regions_with_sites = []

            for region_guid in region_guids:
                # Handle bytes if returned by Redis
                if isinstance(region_guid, bytes):
                    region_guid = region_guid.decode(DEFAULT_ENCODING)

                region_key = f"region:{region_guid}"

                try:
                    region_data = self._get_hash_data(region_key)

                    if region_data and region_data.get("site_guid"):
                        regions_with_sites.append({
                            "region_guid": region_guid,
                            "site_guid": region_data["site_guid"],
                            "created_date": region_data.get(
                                "created_date", ""
                            )
                        })
                except Exception as e:
                    logger.warning(
                        f"Failed to get data for region {region_guid}: {e}"
                    )
                    continue

            if not regions_with_sites:
                logger.debug(
                    f"No regions with site assignments found for "
                    f"asset {asset_guid}"
                )
                return None

            # Find the most recent region by created_date
            latest_region = max(
                regions_with_sites,
                key=lambda r: r["created_date"]
            )

            site_guid = latest_region["site_guid"]

            return site_guid

        except RedisConnectionError:
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error getting site for asset {asset_guid}: {e}"
            )
            raise RedisConnectionError(
                f"Failed to get site for asset {asset_guid}",
                original_exception=e
            ) from e

    def get_site_regions(
        self: Self,
        site_guid: str,
        region_type: str
    ) -> Optional[List[dict]]:
        """
        Retrieve geographic regions of a specific type for a given site.

        This method fetches all regions associated with a site from Redis and
        filters them by the specified region type. It returns detailed region
        information including identifiers and geographic boundaries.

        The method performs the following operations:
        1. Retrieves all regions for the site from a Redis hash
        2. Deserializes JSON data for each region
        3. Filters regions by the specified type (case-insensitive)
        4. Extracts region details including GUID, name, and points

        Args:
            site_guid: Unique identifier for the site whose regions to
                    retrieve. Expected format: UUID string (e.g.,
                    "90f2edf4-72d7-4991-a9a8-f63b5efc7afb")
            region_type: Type of regions to filter for (e.g., "dump", "load").
                        Comparison is case-insensitive.

        Returns:
            List[dict]: List of dictionaries where each dictionary contains:
                    - region_guid (str): Unique identifier for the region
                    - region_name (str): Human-readable name of the region
                    - region_points (str): Comma-separated coordinate pairs
                    Returns None if no matching regions are found.

            Example return value:
            [
                {
                    "region_guid": "abc-123-def",
                    "region_name": "North Dump Area",
                    "region_points": "-71.499290 43.452649,-71.499243 43.452402"
                },
                {
                    "region_guid": "ghi-456-jkl",
                    "region_name": "South Dump Area",
                    "region_points": "-71.507342 43.449508,-71.507097 43.449315"
                }
            ]

            Each region_points string contains:
            - Coordinate pairs separated by commas
            - Each pair in "longitude latitude" format (space-separated)

        Raises:
            RedisConnectionError: If Redis operations fail after all retry
                                attempts.

        Example:
            >>> dao = RedisSourceDAO()
            >>> dump_regions = dao.get_site_regions(
            ...     "90f2edf4-72d7-4991-a9a8-f63b5efc7afb",
            ...     "dump"
            ... )
            >>> if dump_regions:
            ...     print(f"Found {len(dump_regions)} dump regions")
            ...     for region in dump_regions:
            ...         print(f"Region: {region['region_name']} "
            ...               f"(ID: {region['region_guid']})")
            ...         coords = region['region_points'].split(',')
            ...         print(f"  Boundary has {len(coords)} points")

        Note:
            - The region_type comparison is case-insensitive
            - Only regions with both 'region_type' and 'points' fields and
            valid 'region_guid' are included
            - Invalid JSON data for individual regions is logged but doesn't
            fail the operation
            - Coordinate format is "longitude latitude" (not "latitude longitude")
        """
        try:
            # Construct the Redis key for site regions
            site_regions_key = f"site:{site_guid}:regions"

            # Get all regions for the site as a hash
            region_hash = self._get_hash_data(site_regions_key)

            if not region_hash:
                return None

            # Process and filter regions
            matching_regions = []

            for region_name, region_data in region_hash.items():
                try:
                    # Deserialize JSON data
                    region_info = json.loads(region_data)

                    # Check if this region matches the requested type
                    if isinstance(region_info, dict):
                        region_type_value = region_info.get("region_type", "")

                        # Case-insensitive comparison
                        if region_type_value.lower() == region_type.lower():
                            # Extract points if available
                            points = region_info.get("points")
                            region_guid = region_info.get("region_guid")
                            region_name = region_info.get("region_name")
                            if points and region_guid:
                                matching_regions.append(
                                    {
                                        "region_guid": region_guid,
                                        "region_name": region_name,
                                        "region_points": points
                                    }
                                )
                            else:
                                logger.warning(
                                    f"Region '{region_name}' of type "
                                    f"'{region_type}' has no points data "
                                    f"or guid info"
                                )

                except json.JSONDecodeError as e:
                    logger.error(
                        f"Failed to parse JSON for region '{region_name}': {e}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"Unexpected error processing region "
                        f"'{region_name}': {e}"
                    )
                    continue

            if not matching_regions:
                return None

            return matching_regions

        except Exception as e:
            logger.error(
                f"Failed to get regions of type '{region_type}' for site "
                f"{site_guid}: {e}"
            )
            raise RedisConnectionError(
                f"Failed to get site regions for site {site_guid}\n{e}",
                e
            ) from e
