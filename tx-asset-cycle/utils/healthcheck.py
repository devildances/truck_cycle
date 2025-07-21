#!/usr/bin/env python3
"""
Health check script for ao-process container.
Verifies critical dependencies and connectivity.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Fix Python path for imports - try multiple approaches to find the correct path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))

# Add all possible paths
paths_to_try = [
    parent_dir,                                    # /path/to/project
    os.path.dirname(script_dir),                   # /path/to/project
    '/usr/src/app',                                # Docker container path
    os.path.dirname('/usr/src/app'),               # Docker parent path
    os.path.join(os.path.expanduser('~'), 'app')   # Home directory path
]

for path in paths_to_try:
    if path not in sys.path and os.path.exists(path):
        sys.path.insert(0, path)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def log_json(level, message, **kwargs):
    """Output a JSON log line with consistent structure."""
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        "level": level,
        "logger": "healthcheck",
        "pid": os.getpid(),
        "message": message,
        **kwargs
    }
    print(json.dumps(log_entry), flush=True)


def check_imports():
    """Check if all required modules can be imported."""
    try:
        import amazon_kclpy
        import boto3
        import psycopg2
        import redis

        from config.postgresql_config import PGSQL_DB_SETTINGS
        from config.redis_config import REDIS_DB_SETTINGS
        return True, "All imports successful"
    except ImportError as e:
        return False, f"Import failed: {e}"


def check_aws_connectivity():
    """Check if we can reach AWS services."""
    try:
        import boto3
        client = boto3.client('sts')
        identity = client.get_caller_identity()
        return True, f"AWS connectivity OK (Account: {identity['Account']})"
    except Exception as e:
        return False, f"AWS connectivity failed: {str(e)[:100]}"


def check_postgresql():
    """Check PostgreSQL connectivity."""
    try:
        import psycopg2

        from config.postgresql_config import PGSQL_DB_SETTINGS

        conn = psycopg2.connect(**PGSQL_DB_SETTINGS)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
        conn.close()
        return True, "PostgreSQL connection OK"
    except Exception as e:
        return False, f"PostgreSQL failed: {str(e)[:100]}"


def check_redis():
    """Check Redis connectivity (optional - don't fail health check)."""
    try:
        import redis

        from config.redis_config import REDIS_DB_SETTINGS

        host = REDIS_DB_SETTINGS.get("host")
        port = REDIS_DB_SETTINGS.get("port")
        username = REDIS_DB_SETTINGS.get("username")
        password = REDIS_DB_SETTINGS.get("password")

        # Ensure we have proper values
        if not host or not port:
            return False, "Redis config missing host or port"

        r = redis.Redis(
            host=host,
            port=int(port),
            username=username,
            password=password,
            socket_connect_timeout=5,
            ssl=True
        )
        r.ping()
        r.close()
        return True, "Redis connection OK"
    except Exception as e:
        # Redis is optional, so we just warn
        logger.warning(f"Redis check failed (non-critical): {e}")
        return True, f"Redis unavailable (optional): {str(e)[:50]}"


def check_kinesis_access():
    """Check if we can describe the Kinesis stream."""
    try:
        import boto3

        stream_name = os.getenv("KINESIS_STREAM_NAME")
        if not stream_name:
            return False, "KINESIS_STREAM_NAME environment variable not set"

        client = boto3.client('kinesis')
        response = client.describe_stream_summary(StreamName=stream_name)

        # Check if stream is active
        status = response['StreamDescriptionSummary']['StreamStatus']
        if status != 'ACTIVE':
            return False, f"Stream status is {status}, expected ACTIVE"

        return True, f"Kinesis stream '{stream_name}' accessible and ACTIVE"
    except Exception as e:
        return False, f"Kinesis access failed: {str(e)[:100]}"


def main():
    """Run all health checks and report results."""
    start_time = time.time()

    # Log health check start
    log_json("INFO", "Health check started",
             event="healthcheck.start",
             stream=os.getenv("KINESIS_STREAM_NAME", "unknown"))

    checks = [
        ("Import Check", check_imports, "import_check"),
        ("AWS Connectivity", check_aws_connectivity, "aws_connectivity"),
        ("PostgreSQL", check_postgresql, "postgresql"),
        ("Redis Cache", check_redis, "redis"),
        ("Kinesis Stream", check_kinesis_access, "kinesis")
    ]

    all_healthy = True
    check_results = {}
    critical_failures = []

    for name, check_func, check_key in checks:
        try:
            healthy, message = check_func()
            check_results[check_key] = {
                "healthy": healthy,
                "message": message,
                "critical": name in ["Import Check", "AWS Connectivity", "PostgreSQL", "Kinesis Stream"]
            }

            # Only certain checks are critical
            if not healthy and check_results[check_key]["critical"]:
                all_healthy = False
                critical_failures.append(f"{name}: {message}")

        except Exception as e:
            error_msg = str(e)[:100]
            check_results[check_key] = {
                "healthy": False,
                "message": f"Unexpected error - {error_msg}",
                "error": error_msg,
                "critical": name in ["Import Check", "AWS Connectivity", "PostgreSQL", "Kinesis Stream"]
            }
            if check_results[check_key]["critical"]:
                all_healthy = False
                critical_failures.append(f"{name}: {error_msg}")

    # Calculate duration
    duration_ms = int((time.time() - start_time) * 1000)

    # Log health check completion
    log_json(
        "INFO" if all_healthy else "ERROR",
        "Health check completed",
        event="healthcheck.complete",
        healthy=all_healthy,
        duration_ms=duration_ms,
        checks=check_results,
        critical_failures_count=len(critical_failures),
        critical_failures=critical_failures if critical_failures else None
    )

    if all_healthy:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()