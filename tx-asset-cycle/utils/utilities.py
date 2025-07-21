import json
import logging
import math
import traceback
from datetime import datetime, timezone
from typing import List, Optional, Union

import boto3

logger = logging.getLogger(__name__)


def get_stack_trace_py(ex: Exception) -> str:
    """Generate a formatted string representation of an exception's stack trace.

    Extracts and formats the complete stack trace from an exception object,
    providing detailed debugging information including file paths, line numbers,
    and the sequence of function calls that led to the exception.

    Args:
        ex (Exception): The exception object to extract stack trace from.
            If None or falsy, returns a default message.

    Returns:
        str: A formatted multi-line string containing the complete stack trace,
            or "No exception provided." if no exception is given.

    Example:
        >>> try:
        ...     raise ValueError("Sample error")
        ... except Exception as e:
        ...     trace = get_stack_trace_py(e)
        ...     print(trace)
        Traceback (most recent call last):
          File "<stdin>", line 2, in <module>
        ValueError: Sample error
    """
    if ex:
        return "".join(traceback.format_exception(type(ex), ex, ex.__traceback__))
    return "No exception provided."


def format_log_elapse_time(total_seconds: Union[int, float]) -> str:
    """Convert a duration in seconds to a standardized HH:MM:SS.sssss format.

    Formats elapsed time for consistent logging and display purposes.
    The output uses zero-padded hours and minutes, with microsecond
    precision for seconds.

    Args:
        total_seconds (Union[int, float]): Duration in seconds to format.
            Must be non-negative numeric value.

    Returns:
        str: Formatted time string in HH:MM:SS.sssss format where:
            - HH: Zero-padded hours (00-99+)
            - MM: Zero-padded minutes (00-59)
            - SS.sssss: Seconds with 5 decimal places for microseconds

    Raises:
        TypeError: If total_seconds is not a number (int or float).
        ValueError: If total_seconds is negative.

    Example:
        >>> format_log_elapse_time(3661.12345)
        '01:01:01.12345'
        >>> format_log_elapse_time(45.6)
        '00:00:45.60000'
    """
    if not isinstance(total_seconds, (int, float)):
        raise TypeError("Duration must be a number.")
    if total_seconds < 0:
        raise ValueError("Duration must be non-negative.")

    hours = int(total_seconds // 3600)
    remainder = total_seconds % 3600
    minutes = int(remainder // 60)
    seconds_with_fraction = remainder % 60

    return f"{hours:02d}:{minutes:02d}:{seconds_with_fraction:08.5f}"


def get_db_credentials_from_secrets_manager(
    secret_name: str,
    region_name: str,
    db_type: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None
) -> dict:
    """Retrieve database credentials from AWS Secrets Manager.

    Connects to AWS Secrets Manager to fetch database connection credentials
    and formats them according to the specified database type. Supports both
    PostgreSQL and Redis credential structures.

    Args:
        secret_name (str): Name/ARN of the secret in AWS Secrets Manager.
        region_name (str): AWS region where the secret is stored.
        db_type (str): Database type identifier ('pgsql' or 'redis').
            Case-insensitive.
        aws_access_key_id (Optional[str]): AWS access key ID for
            authentication. If None, uses default AWS credential chain.
        aws_secret_access_key (Optional[str]): AWS secret access key for
            authentication. Required if aws_access_key_id is provided.
        aws_session_token (Optional[str]): AWS session token for temporary
            credentials. Used with access key ID and secret access key.

    Returns:
        dict: Database connection credentials with keys depending on db_type:
            - For 'pgsql': host, port, database, user, password
            - For 'redis': host, port, username, password

    Raises:
        Exception: If AWS client initialization fails, secret retrieval fails,
            JSON parsing fails, or required credential keys are missing.
        ValueError: If db_type is not 'pgsql' or 'redis'.
        KeyError: If expected credential keys are missing from the secret.

    Example:
        >>> creds = get_db_credentials_from_secrets_manager(
        ...     "my-db-secret",
        ...     "us-east-1",
        ...     "pgsql"
        ... )
        >>> print(creds)
        {'host': 'db.example.com', 'port': '5432', 'database': 'mydb',
         'user': 'admin', 'password': 'secret123'}
    """
    try:
        if aws_access_key_id and aws_secret_access_key:
            session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name
            )
        else:
            session = boto3.session.Session(region_name=region_name)

        client = session.client(
            service_name='secretsmanager'
        )

        logger.info(
            "Attempting to retrieve secret: "
            f"{secret_name} from region: {region_name}"
        )
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    except Exception as e:
        logger.error(
            "ERROR: Could not initialize Boto3 client "
            f"or retrieve secret '{secret_name}'. Error: {e}"
        )
        raise Exception(
            f"Failed to retrieve secret from AWS Secrets Manager: {e}"
        ) from e

    if 'SecretString' in get_secret_value_response:
        secret_string = get_secret_value_response['SecretString']
        try:
            secret_data = json.loads(secret_string)
            if db_type.lower() == "pgsql":
                credentials = {
                    "host": secret_data.get("host"),
                    "port": str(secret_data.get("port", "5432")),
                    "database": secret_data.get("dbname"),
                    "user": secret_data.get("username"),
                    "password": secret_data.get("password")
                }
            elif db_type.lower() == "redis":
                credentials = {
                    "host": secret_data.get("host"),
                    "port": secret_data.get("port"),
                    "username": secret_data.get("username"),
                    "password": secret_data.get("password")
                }
            else:
                raise ValueError(f"{db_type} is unknown for database type value.")
            missing_keys = [k for k, v in credentials.items() if v is None]
            if missing_keys:
                raise KeyError(
                    "Missing expected keys in secret "
                    f"{secret_name} data: {', '.join(missing_keys)}"
                )
            return credentials
        except json.JSONDecodeError as e:
            logger.error(
                "ERROR: Could not parse secret "
                f"{secret_name} string as JSON. Error: {e}"
            )
            raise Exception(
                f"Failed to parse secret {secret_name} JSON: {e}"
            ) from e
        except KeyError as e:
            logger.error(
                f"ERROR: Secret {secret_name} data is "
                f"missing expected keys. Error: {e}"
            )
            raise Exception(
                f"Secret {secret_name} data structure error: {e}"
            ) from e
    else:
        logger.error(
            f"ERROR: {secret_name} SecretString not "
            "found in AWS Secrets Manager response."
        )
        raise Exception(
            f"{secret_name} SecretString not found in AWS response."
        )


def parse_iso_timestamp_to_naive_utc(timestamp_str: str) -> datetime:
    """Parse an ISO 8601 timestamp string to a naive UTC datetime object.

    Converts various ISO 8601 timestamp formats to a standardized naive
    datetime object in UTC timezone. Handles both timezone-aware and
    timezone-naive inputs, converting timezone-aware timestamps to UTC
    and treating naive timestamps as UTC.

    Args:
        timestamp_str (str): ISO 8601 formatted timestamp string.
            Supports formats like '2024-01-01T10:30:00Z',
            '2024-01-01T10:30:00+05:00', or '2024-01-01T10:30:00'.

    Returns:
        datetime: Naive datetime object representing the timestamp in UTC.
            The returned datetime has tzinfo=None.

    Raises:
        ValueError: If the timestamp string is not in valid ISO 8601 format
            or cannot be parsed.

    Example:
        >>> dt = parse_iso_timestamp_to_naive_utc("2024-01-01T10:30:00Z")
        >>> print(dt)
        2024-01-01 10:30:00
        >>> print(dt.tzinfo)
        None

        >>> dt = parse_iso_timestamp_to_naive_utc("2024-01-01T10:30:00+05:00")
        >>> print(dt)  # Converted to UTC
        2024-01-01 05:30:00
    """
    try:
        dt_obj = datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError):
        raise ValueError(
            f"Invalid ISO 8601 format for timestamp: '{timestamp_str}'"
        )

    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return dt_obj.replace(tzinfo=timezone.utc).replace(tzinfo=None)
    else:
        return dt_obj.astimezone(timezone.utc).replace(tzinfo=None)


def timestamp_to_utc_zero(target_timestamp: datetime) -> datetime:
    """Convert a datetime object to UTC timezone or add UTC timezone info.

    Ensures datetime objects have UTC timezone information. For naive datetime
    objects (no timezone info), adds UTC timezone. For timezone-aware objects,
    returns them unchanged.

    Args:
        target_timestamp (datetime): The datetime object to process.
            Can be timezone-aware or timezone-naive.

    Returns:
        datetime: A timezone-aware datetime object in UTC.
            - If input was naive, returns the same time with UTC timezone
              added
            - If input was timezone-aware, returns it unchanged

    Example:
        >>> from datetime import datetime, timezone
        >>> naive_dt = datetime(2024, 1, 1, 10, 30, 0)
        >>> utc_dt = timestamp_to_utc_zero(naive_dt)
        >>> print(utc_dt.tzinfo)
        datetime.timezone.utc

        >>> aware_dt = datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        >>> result = timestamp_to_utc_zero(aware_dt)
        >>> print(result is aware_dt)
        True
    """
    if (
        isinstance(target_timestamp, datetime)
        and target_timestamp.tzinfo is None
    ):
        return target_timestamp.replace(tzinfo=timezone.utc)
    return target_timestamp


def validate_coordinates(
    point: List[float],
    point_name: str = "Point"
) -> None:
    """Validate latitude and longitude coordinates.

    Args:
        point: List containing [latitude, longitude]
        point_name: Name of the point for error messages

    Raises:
        ValueError: If coordinates are invalid
        TypeError: If point is not a list or doesn't contain numbers
    """
    if not isinstance(point, list):
        raise TypeError(f"{point_name} must be a list")

    if len(point) != 2:
        raise ValueError(
            f"{point_name} must contain exactly 2 values [latitude, longitude]"
        )

    try:
        lat, lon = float(point[0]), float(point[1])
    except (ValueError, TypeError) as exc:
        raise TypeError(
            f"{point_name} coordinates must be numeric values"
        ) from exc

    if not (-90.0 <= lat <= 90.0):
        raise ValueError(
            f"{point_name} latitude must be between -90 and 90 degrees, got {lat}"
        )

    if not (-180.0 <= lon <= 180.0):
        raise ValueError(
            f"{point_name} longitude must be between -180 and 180 degrees, "
            f"got {lon}"
        )


def validate_distance_threshold(distance: float) -> None:
    """Validate distance threshold value.

    Args:
        distance: Distance threshold in meters

    Raises:
        ValueError: If distance is negative
        TypeError: If distance is not numeric
    """
    try:
        distance = float(distance)
    except (ValueError, TypeError) as exc:
        raise TypeError(
            "Distance threshold must be a numeric value"
        ) from exc

    if distance < 0:
        raise ValueError("Distance threshold must be non-negative")


def haversine_distance(
    point1: List[float],
    point2: List[float]
) -> float:
    """
    Calculate the great circle distance between two points on
    Earth using the Haversine formula.

    Args:
        point1: List containing [lat, lon] of first point
        point2: List containing [lat, lon] of second point

    Returns:
        Distance between the two points in meters

    Raises:
        ValueError: If coordinates are invalid
        TypeError: If points are not properly formatted
    """
    # Validate input coordinates
    validate_coordinates(point1, "Point 1")
    validate_coordinates(point2, "Point 2")

    # Earth's radius in meters
    EARTH_RADIUS = 6371000.0

    # Extract coordinates
    lat1, lon1 = point1
    lat2, lon2 = point2

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    form_w = math.sin(delta_lat / 2) ** 2
    form_x = math.cos(lat1_rad)
    form_y = math.cos(lat2_rad)
    form_z = math.sin(delta_lon / 2) ** 2

    # Haversine formula
    a = form_w + form_x * form_y * form_z

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Calculate distance in meters
    distance = EARTH_RADIUS * c

    return distance


class DurationCalculator:
    """Utility class for duration calculations.

    Provides methods for calculating time durations between timestamps,
    which are essential for tracking cycle segment durations and idle times.

    All duration calculations return values in seconds as floats.

    Example:
        >>> calc = DurationCalculator()
        >>> duration = calc.calculate_seconds(start_time, end_time)
        >>> idle_time = calc.calculate_idle_duration(last_update)
    """

    @staticmethod
    def calculate_seconds(
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> Optional[float]:
        """Calculate duration in seconds between timestamps.

        Computes the time difference between two timestamps, handling
        timezone normalization through the timestamp_to_utc_zero utility.

        Args:
            start_time: The starting timestamp (can be None)
            end_time: The ending timestamp (can be None)

        Returns:
            Optional[float]: Duration in seconds if both timestamps exist,
                           None if either timestamp is None

        Example:
            >>> duration = calculate_seconds(
            ...     datetime(2024, 1, 1, 10, 0, 0),
            ...     datetime(2024, 1, 1, 10, 5, 30)
            ... )
            >>> print(duration)  # 330.0
        """
        if start_time and end_time:
            start = timestamp_to_utc_zero(start_time)
            end = timestamp_to_utc_zero(end_time)
            return (end - start).total_seconds()
        return None

    @staticmethod
    def calculate_idle_duration(last_updated: datetime) -> float:
        """Calculate idle duration from last update to now.

        Computes how long a truck has been idle by calculating the time
        difference between the last update timestamp and the current time.

        Args:
            last_updated: Timestamp of the last record update

        Returns:
            float: Duration in seconds since last update

        Note:
            This method always returns a value (never None) as it uses
            the current time as the end point.

        Example:
            >>> last_update = datetime.now(timezone.utc) - timedelta(minutes=5)
            >>> idle_seconds = calculate_idle_duration(last_update)
            >>> print(idle_seconds)  # ~300.0
        """
        now = datetime.now(timezone.utc)
        last = timestamp_to_utc_zero(last_updated)
        return (now - last).total_seconds()
