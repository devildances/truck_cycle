#!/usr/bin/env python3
"""
Health check script for ao-process container.
Verifies critical dependencies and connectivity.
"""
# Debug output BEFORE any imports to catch import failures
import sys

print("[HEALTHCHECK] Script starting...", file=sys.stderr, flush=True)

try:
    import json
    import logging
    import os
    import time
    from datetime import datetime, timezone
    print("[HEALTHCHECK] Basic imports successful", file=sys.stderr, flush=True)
except Exception as e:
    print(f"[HEALTHCHECK] FATAL: Failed to import basic modules: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

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

# Set up logging to stdout only at WARNING level
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def log_json(level, message, **kwargs):
    """Output a JSON log line with consistent structure."""
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        "level": level,
        "logger": "healthcheck",
        "pid": os.getpid(),
        "message": message,
        "health_check": True,  # Tag for easy CloudWatch filtering
        "service": os.getenv("TX_TYPE", "unknown"),
        **kwargs
    }
    # Output to stdout only
    print(json.dumps(log_entry), flush=True)


def check_imports():
    """Check if all required modules can be imported."""
    print("[HEALTHCHECK] Checking imports...", file=sys.stderr, flush=True)
    try:
        import amazon_kclpy  # noqa: F401
        print("[HEALTHCHECK] amazon_kclpy imported", file=sys.stderr, flush=True)
        import boto3  # noqa: F401
        print("[HEALTHCHECK] boto3 imported", file=sys.stderr, flush=True)
        import psycopg2  # noqa: F401
        print("[HEALTHCHECK] psycopg2 imported", file=sys.stderr, flush=True)
        import redis  # noqa: F401
        print("[HEALTHCHECK] redis imported", file=sys.stderr, flush=True)

        from config.postgresql_config import PGSQL_DB_SETTINGS  # noqa: F401
        print("[HEALTHCHECK] postgresql_config imported", file=sys.stderr, flush=True)
        from config.redis_config import REDIS_DB_SETTINGS  # noqa: F401
        print("[HEALTHCHECK] redis_config imported", file=sys.stderr, flush=True)
        return True, "All imports successful"
    except ImportError as e:
        module_name = str(e.name) if hasattr(e, 'name') else "unknown"
        print(f"[HEALTHCHECK] Import failed: {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "Import failed", error=str(e), module=module_name)
        return False, f"Import failed: {e}"


def check_aws_connectivity():
    """Check if we can reach AWS services."""
    print("[HEALTHCHECK] Starting AWS connectivity check...", file=sys.stderr, flush=True)
    try:
        import boto3
        from botocore.exceptions import (ClientError, NoCredentialsError,
                                         PartialCredentialsError)
        print("[HEALTHCHECK] AWS boto3 imports successful", file=sys.stderr, flush=True)

        client = boto3.client('sts')
        print("[HEALTHCHECK] AWS STS client created", file=sys.stderr, flush=True)
        identity = client.get_caller_identity()
        print("[HEALTHCHECK] AWS get_caller_identity successful", file=sys.stderr, flush=True)
        account_id = identity.get('Account', 'unknown')
        print(f"[HEALTHCHECK] AWS connectivity check passed (Account: {account_id})", file=sys.stderr, flush=True)
        return True, f"AWS connectivity OK (Account: {account_id})"
    except NoCredentialsError as e:
        print(f"[HEALTHCHECK] AWS NoCredentialsError: {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "AWS credentials not found",
                 error=str(e), error_type="NoCredentialsError")
        return False, f"AWS credentials not found: {str(e)[:100]}"
    except PartialCredentialsError as e:
        print(f"[HEALTHCHECK] AWS PartialCredentialsError: {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "AWS partial credentials",
                 error=str(e), error_type="PartialCredentialsError")
        return False, f"AWS credentials incomplete: {str(e)[:100]}"
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"[HEALTHCHECK] AWS ClientError ({error_code}): {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "AWS client error", error=str(e),
                 error_type="ClientError", error_code=error_code)
        return False, f"AWS client error: {str(e)[:100]}"
    except Exception as e:
        print(f"[HEALTHCHECK] AWS unexpected error ({type(e).__name__}): {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "AWS connectivity failed",
                 error=str(e), error_type=type(e).__name__)
        return False, f"AWS connectivity failed: {str(e)[:100]}"


def check_postgresql():
    """Check PostgreSQL connectivity."""
    print("[HEALTHCHECK] Starting PostgreSQL connectivity check...", file=sys.stderr, flush=True)
    try:
        import psycopg2
        print("[HEALTHCHECK] psycopg2 import successful", file=sys.stderr, flush=True)

        from config.postgresql_config import PGSQL_DB_SETTINGS
        print("[HEALTHCHECK] PostgreSQL config imported", file=sys.stderr, flush=True)

        conn = psycopg2.connect(**PGSQL_DB_SETTINGS)
        print("[HEALTHCHECK] PostgreSQL connection established", file=sys.stderr, flush=True)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            print("[HEALTHCHECK] PostgreSQL test query executed", file=sys.stderr, flush=True)
            cur.fetchone()  # Fetch result but don't need to use it
            print("[HEALTHCHECK] PostgreSQL test query result fetched", file=sys.stderr, flush=True)
        conn.close()
        print("[HEALTHCHECK] PostgreSQL connection closed successfully", file=sys.stderr, flush=True)
        return True, "PostgreSQL connection OK"
    except psycopg2.OperationalError as e:
        print(f"[HEALTHCHECK] PostgreSQL OperationalError: {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "PostgreSQL connection failed",
                 error=str(e), error_type="OperationalError")
        return False, f"Database connection failed: {str(e)[:100]}"
    except psycopg2.Error as e:
        print(f"[HEALTHCHECK] PostgreSQL Error ({type(e).__name__}): {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "PostgreSQL error",
                 error=str(e), error_type=type(e).__name__)
        return False, f"PostgreSQL error: {str(e)[:100]}"
    except Exception as e:
        print(f"[HEALTHCHECK] PostgreSQL unexpected error ({type(e).__name__}): {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "Unexpected PostgreSQL error",
                 error=str(e), error_type=type(e).__name__)
        return False, f"Unexpected error: {str(e)[:100]}"


def check_redis():
    """Check Redis connectivity (optional - don't fail health check)."""
    print("[HEALTHCHECK] Starting Redis connectivity check...", file=sys.stderr, flush=True)
    try:
        import redis
        print("[HEALTHCHECK] redis import successful", file=sys.stderr, flush=True)

        from config.redis_config import REDIS_DB_SETTINGS
        print("[HEALTHCHECK] Redis config imported", file=sys.stderr, flush=True)

        host = REDIS_DB_SETTINGS.get("host")
        port = REDIS_DB_SETTINGS.get("port")
        username = REDIS_DB_SETTINGS.get("username")
        password = REDIS_DB_SETTINGS.get("password")
        print(f"[HEALTHCHECK] Redis config values extracted: host={host}, port={port}", file=sys.stderr, flush=True)

        # Ensure we have proper values
        if not host or not port:
            print("[HEALTHCHECK] Redis config missing host or port", file=sys.stderr, flush=True)
            return False, "Redis config missing host or port"

        r = redis.Redis(
            host=host,
            port=int(port),
            username=username,
            password=password,
            socket_connect_timeout=5,
            ssl=True
        )
        print("[HEALTHCHECK] Redis client created, attempting ping...", file=sys.stderr, flush=True)
        r.ping()
        print("[HEALTHCHECK] Redis ping successful", file=sys.stderr, flush=True)
        r.close()
        print("[HEALTHCHECK] Redis connection closed successfully", file=sys.stderr, flush=True)
        return True, "Redis connection OK"
    except redis.ConnectionError as e:
        # Redis is optional, so we just warn
        print(f"[HEALTHCHECK] Redis ConnectionError (non-critical): {e}", file=sys.stderr, flush=True)
        logger.warning(f"Redis connection failed (non-critical): {e}")
        log_json("WARNING", "Redis connection failed",
                 error=str(e), error_type="ConnectionError")
        return True, f"Redis connection failed: {str(e)[:50]}"
    except redis.TimeoutError as e:
        print(f"[HEALTHCHECK] Redis TimeoutError (non-critical): {e}", file=sys.stderr, flush=True)
        logger.warning(f"Redis timeout (non-critical): {e}")
        log_json("WARNING", "Redis timeout",
                 error=str(e), error_type="TimeoutError")
        return True, f"Redis timeout: {str(e)[:50]}"
    except redis.RedisError as e:
        print(f"[HEALTHCHECK] Redis RedisError (non-critical): {e}", file=sys.stderr, flush=True)
        logger.warning(f"Redis error (non-critical): {e}")
        log_json("WARNING", "Redis error",
                 error=str(e), error_type=type(e).__name__)
        return True, f"Redis error: {str(e)[:50]}"
    except Exception as e:
        # Redis is optional, so we just warn
        print(f"[HEALTHCHECK] Redis unexpected error (non-critical): {e}", file=sys.stderr, flush=True)
        logger.warning(f"Redis check failed (non-critical): {e}")
        log_json("WARNING", "Unexpected Redis error",
                 error=str(e), error_type=type(e).__name__)
        return True, f"Redis unavailable (optional): {str(e)[:50]}"


def check_kinesis_access():
    """Check if we can describe the Kinesis stream."""
    print("[HEALTHCHECK] Starting Kinesis access check...", file=sys.stderr, flush=True)
    try:
        import boto3
        from botocore.exceptions import ClientError
        print("[HEALTHCHECK] Kinesis boto3 imports successful", file=sys.stderr, flush=True)

        stream_name = os.getenv("KINESIS_STREAM_NAME")
        print(f"[HEALTHCHECK] KINESIS_STREAM_NAME: {stream_name}", file=sys.stderr, flush=True)

        if not stream_name:
            print("[HEALTHCHECK] KINESIS_STREAM_NAME not set", file=sys.stderr, flush=True)
            log_json("ERROR", "KINESIS_STREAM_NAME not set")
            return False, "KINESIS_STREAM_NAME environment variable not set"

        client = boto3.client('kinesis')
        print("[HEALTHCHECK] Kinesis client created", file=sys.stderr, flush=True)
        response = client.describe_stream_summary(StreamName=stream_name)
        print("[HEALTHCHECK] Kinesis describe_stream_summary successful", file=sys.stderr, flush=True)

        # Check if stream is active
        status = response['StreamDescriptionSummary']['StreamStatus']
        print(f"[HEALTHCHECK] Kinesis stream status: {status}", file=sys.stderr, flush=True)

        if status != 'ACTIVE':
            print(f"[HEALTHCHECK] Kinesis stream not active: {status} (expected ACTIVE)", file=sys.stderr, flush=True)
            log_json("ERROR", "Stream not active",
                     status=status, expected="ACTIVE")
            return False, f"Stream status is {status}, expected ACTIVE"

        print(f"[HEALTHCHECK] Kinesis check passed - stream '{stream_name}' is ACTIVE", file=sys.stderr, flush=True)
        return True, f"Kinesis stream '{stream_name}' accessible and ACTIVE"
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"[HEALTHCHECK] Kinesis ClientError ({error_code}): {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "Kinesis client error", error=str(e),
                 error_type="ClientError", error_code=error_code)
        if error_code == 'ResourceNotFoundException':
            return False, f"Kinesis stream not found: {str(e)[:100]}"
        elif error_code == 'AccessDeniedException':
            return False, f"Access denied to Kinesis stream: {str(e)[:100]}"
        else:
            return False, f"Kinesis client error ({error_code}): {str(e)[:100]}"
    except Exception as e:
        print(f"[HEALTHCHECK] Kinesis unexpected error ({type(e).__name__}): {e}", file=sys.stderr, flush=True)
        log_json("ERROR", "Kinesis access failed",
                 error=str(e), error_type=type(e).__name__)
        return False, f"Kinesis access failed: {str(e)[:100]}"


def main():
    """Run all health checks and report results."""
    print("[HEALTHCHECK] Main function starting...", file=sys.stderr, flush=True)
    start_time = time.time()

    # Log health check start
    print("[HEALTHCHECK] Logging health check start event", file=sys.stderr, flush=True)
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
    print(f"[HEALTHCHECK] Running {len(checks)} health checks", file=sys.stderr, flush=True)

    all_healthy = True
    check_results = {}
    critical_failures = []

    for name, check_func, check_key in checks:
        print(f"[HEALTHCHECK] Starting check: {name}", file=sys.stderr, flush=True)
        try:
            healthy, message = check_func()
            print(f"[HEALTHCHECK] Check '{name}' completed: healthy={healthy}, message={message}", file=sys.stderr, flush=True)
            check_results[check_key] = {
                "healthy": healthy,
                "message": message,
                "critical": name in ["Import Check", "AWS Connectivity",
                                     "PostgreSQL", "Kinesis Stream"]
            }

            # Only certain checks are critical
            if not healthy and check_results[check_key]["critical"]:
                print(f"[HEALTHCHECK] Critical check '{name}' FAILED: {message}", file=sys.stderr, flush=True)
                all_healthy = False
                critical_failures.append(f"{name}: {message}")
            elif not healthy:
                print(f"[HEALTHCHECK] Non-critical check '{name}' failed: {message}", file=sys.stderr, flush=True)

        except Exception as e:
            error_msg = str(e)[:100]
            print(f"[HEALTHCHECK] Check '{name}' CRASHED with exception: {e}", file=sys.stderr, flush=True)
            log_json("ERROR", f"Check crashed: {name}",
                     check_name=name, error=str(e),
                     error_type=type(e).__name__)
            check_results[check_key] = {
                "healthy": False,
                "message": f"Unexpected error - {error_msg}",
                "error": error_msg,
                "critical": name in ["Import Check", "AWS Connectivity",
                                     "PostgreSQL", "Kinesis Stream"]
            }
            if check_results[check_key]["critical"]:
                print(f"[HEALTHCHECK] Critical check '{name}' CRASHED: {error_msg}", file=sys.stderr, flush=True)
                all_healthy = False
                critical_failures.append(f"{name}: {error_msg}")

    # Calculate duration
    duration_ms = int((time.time() - start_time) * 1000)
    print(f"[HEALTHCHECK] All checks completed in {duration_ms}ms", file=sys.stderr, flush=True)

    # Log health check completion
    print(f"[HEALTHCHECK] Final result: all_healthy={all_healthy}, critical_failures={len(critical_failures)}", file=sys.stderr, flush=True)
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
        print("[HEALTHCHECK] Exiting with status 0 (healthy)", file=sys.stderr, flush=True)
        sys.exit(0)
    else:
        print(f"[HEALTHCHECK] Exiting with status 1 (unhealthy) - {len(critical_failures)} critical failures", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
