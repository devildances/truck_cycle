import logging
from dataclasses import dataclass, field
from typing import List, Optional, Self, Tuple

from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry.base import GEOSException

from utils.utilities import get_stack_trace_py

logger = logging.getLogger(__name__)


@dataclass
class RegionPoly:
    """Geographical region with polygon operations for mining sites.

    Represents a defined geographical area within a mining site, such as
    dump regions, loading zones, or restricted areas. Handles parsing of
    coordinate string data and provides spatial operations through Shapely
    polygon geometry.

    The class supports point-in-polygon testing for determining if mining
    assets (trucks, loaders) are within specific operational zones.
    Coordinates are expected in longitude/latitude format and are cached
    after initial parsing for performance.

    Key Features:
        - Parses comma-separated coordinate strings into Shapely polygons
        - Caches parsed polygons to avoid recomputation
        - Validates polygon geometry and logs warnings for invalid shapes
        - Provides detailed error messages for debugging coordinate issues
        - Supports both load and dump site classifications

    Attributes:
        region_guid (str): Unique identifier for this geographical region.
        site_guid (Optional[str]): Identifier of the parent mining site.
        name (Optional[str]): Human-readable name for the region.
        fence_area (Optional[float]): Calculated area of the region in
            appropriate units (typically square meters).
        region_points (Optional[str]): Comma-separated coordinate string
            in format "lon1 lat1, lon2 lat2, lon3 lat3, ...". Hidden from
            repr due to potential length.
        load_site (Optional[bool]): Whether this region is designated for
            loading operations.
        dump_site (Optional[bool]): Whether this region is designated for
            dumping operations.
        region_type (Optional[str]): Classification of region type (e.g.,
            "DUMP_ZONE", "LOAD_ZONE", "RESTRICTED").
        is_deleted (Optional[bool]): Soft delete flag for region status.

    Private Attributes:
        _shapely_polygon (Optional[ShapelyPolygon]): Cached Shapely polygon
            object after successful parsing. Not included in initialization
            or string representation.
        _parse_error_message (Optional[str]): Error message from last parse
            attempt, used for debugging and error reporting. Not included
            in initialization or string representation.

    Example:
        >>> # Define a triangular dump region
        >>> region = RegionPoly(
        ...     region_guid="dump-001",
        ...     site_guid="site-123",
        ...     name="Main Dump Zone",
        ...     region_points="-122.4 37.7, -122.3 37.7, -122.35 37.8",
        ...     dump_site=True,
        ...     region_type="DUMP_ZONE"
        ... )
        >>> polygon = region.get_polygon()
        >>> if polygon:
        ...     # Test if a point is within the region
        ...     from shapely.geometry import Point
        ...     test_point = Point(-122.35, 37.75)
        ...     is_inside = polygon.contains(test_point)
        ...     print(f"Point is inside region: {is_inside}")

    Coordinate Format:
        The region_points string should contain coordinates as:
        "longitude1 latitude1, longitude2 latitude2, longitude3 latitude3"

        Example: "-122.4194 37.7749, -122.4094 37.7849, -122.4294 37.7849"

        - Coordinates must be space-separated within each pair
        - Pairs must be comma-separated
        - At least 3 coordinate pairs required for valid polygon
        - Longitude typically negative for western hemisphere
        - Latitude typically positive for northern hemisphere
    """

    region_guid: str
    site_guid: Optional[str] = None
    name: Optional[str] = None
    fence_area: Optional[float] = None
    region_points: Optional[str] = field(default=None, repr=False)

    load_site: Optional[bool] = None
    dump_site: Optional[bool] = None
    region_type: Optional[str] = field(default=None)
    is_deleted: Optional[bool] = None

    _shapely_polygon: Optional[ShapelyPolygon] = field(
        init=False, repr=False, default=None
    )
    _parse_error_message: Optional[str] = field(
        init=False, repr=False, default=None
    )

    def get_polygon(self: Self) -> Optional[ShapelyPolygon]:
        """Parse region coordinates and return a Shapely polygon for operations.

        Parses the region_points string into a Shapely Polygon object suitable
        for spatial operations like point-in-polygon testing, intersection
        calculations, and area computations. The result is cached to avoid
        re-parsing on subsequent calls.

        The parsing process:
        1. Splits region_points by commas to get coordinate pairs
        2. Splits each pair by spaces to get longitude/latitude values
        3. Converts string coordinates to float values
        4. Validates minimum 3 points required for polygon
        5. Creates Shapely Polygon and validates geometry
        6. Caches result for future calls

        Returns:
            Optional[ShapelyPolygon]: Valid Shapely polygon if parsing
                succeeds and coordinates form a valid polygon. Returns None
                if parsing fails, insufficient points provided, or geometry
                is invalid.

        Caching Behavior:
            - Successful polygons are cached in _shapely_polygon
            - Parse errors are cached in _parse_error_message
            - Subsequent calls return cached results without re-parsing
            - Cache persists for the lifetime of the object

        Error Handling:
            The method handles various error conditions gracefully:
            - Empty or None region_points string
            - Invalid coordinate format (non-numeric values)
            - Insufficient coordinate pairs (< 3 required)
            - Shapely/GEOS geometry errors
            - Malformed coordinate pair format

        Validation:
            - Checks polygon validity using Shapely's is_valid property
            - Logs warnings for invalid polygons but still returns them
            - Invalid polygons may still work for some operations

        Example:
            >>> region = RegionPoly(
            ...     region_guid="test-region",
            ...     region_points="-122.4 37.7, -122.3 37.7, -122.35 37.8"
            ... )
            >>> polygon = region.get_polygon()
            >>> if polygon:
            ...     print(f"Polygon area: {polygon.area}")
            ...     print(f"Polygon is valid: {polygon.is_valid}")
            ... else:
            ...     print(f"Parse error: {region._parse_error_message}")

        Performance Notes:
            - First call performs parsing and validation
            - Subsequent calls return cached polygon (O(1) operation)
            - Failed parsing is also cached to avoid repeated attempts
            - Large polygons with many points may take longer to parse

        Coordinate Requirements:
            - Format: "lon1 lat1, lon2 lat2, lon3 lat3, ..."
            - Minimum 3 coordinate pairs for valid polygon
            - Coordinates must be valid floating-point numbers
            - Longitude/latitude order is enforced
        """
        if self._shapely_polygon:
            return self._shapely_polygon
        if self._parse_error_message:
            return None
        if not self.region_points or not self.region_points.strip():
            self._parse_error_message = "Points string is null or empty."
            return None

        try:
            coord_pairs_str = self.region_points.split(",")
            coordinates: List[Tuple[float, float]] = []

            for cs_pair in coord_pairs_str:
                cs_pair_stripped = cs_pair.strip()
                if not cs_pair_stripped:
                    continue

                parts = cs_pair_stripped.split(" ")
                if len(parts) == 2:
                    try:
                        lon = float(parts[0])
                        lat = float(parts[1])
                        coordinates.append((lon, lat))
                    except ValueError:
                        error_msg = (
                            f"Invalid coordinate value in pair: '{cs_pair_stripped}'"
                        )
                        self._parse_error_message = error_msg
                        logger.error(
                            f"Region {self.region_guid}: {self._parse_error_message}"
                        )
                        return None
                else:
                    error_msg = (
                        f"Invalid coordinate format (expected 'lon lat'): "
                        f"'{cs_pair_stripped}'"
                    )
                    self._parse_error_message = error_msg
                    logger.error(
                        f"Region {self.region_guid}: {self._parse_error_message}"
                    )
                    return None

            if not coordinates:
                self._parse_error_message = (
                    "No valid coordinates found after parsing region_points."
                )
                logger.warning(
                    f"Region {self.region_guid}: {self._parse_error_message} "
                    f"from region_points: '{self.region_points}'"
                )
                return None

            if len(coordinates) < 3:
                self._parse_error_message = (
                    f"Insufficient points to form a polygon "
                    f"(need at least 3, got {len(coordinates)})."
                )
                logger.warning(
                    f"Region {self.region_guid}: {self._parse_error_message} "
                    f"from region_points: '{self.region_points}'"
                )
                return None

            self._shapely_polygon = ShapelyPolygon(coordinates)

            if not self._shapely_polygon.is_valid:
                reason = "unknown"
                try:
                    reason = self._shapely_polygon.is_valid_reason
                except AttributeError:
                    pass
                logger.warning(
                    f"Region {self.region_guid}: Constructed polygon is invalid "
                    f"according to Shapely. Reason: {reason}. Operations might "
                    f"still work or yield unexpected results."
                )
            return self._shapely_polygon

        except GEOSException as e:
            self._parse_error_message = (
                f"Shapely/GEOS error during polygon creation: {e}"
            )
            stack_trace = get_stack_trace_py(e)
            logger.error(
                f"Region {self.region_guid}: {self._parse_error_message} "
                f"for points '{self.region_points}'. Trace: {stack_trace}"
            )
            return None
        except Exception as e:
            self._parse_error_message = (
                f"Unexpected error parsing points or creating polygon: {e}"
            )
            stack_trace = get_stack_trace_py(e)
            logger.error(
                f"Region {self.region_guid}: {self._parse_error_message} "
                f"for points '{self.region_points}'. Trace: {stack_trace}"
            )
            return None
