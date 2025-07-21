#!/usr/bin/env python3
import logging
import os
import signal
import subprocess
import sys
import time
from glob import glob
from types import FrameType
from typing import Optional

import psutil

# Force unbuffered output
os.environ['PYTHONUNBUFFERED'] = '1'

# Print startup message immediately
print(
    f"[KCL-HELPER] Starting at {time.strftime('%Y-%m-%d %H:%M:%S')} PID={os.getpid()}",
    flush=True
)

logger = logging.getLogger(__name__)


def log_memory_usage() -> None:
    """Logs the memory usage of the current Python process and the Java KCL daemon."""
    try:
        current_process = psutil.Process()
        python_mem_mb = current_process.memory_info().rss / (1024 * 1024)
        print(
            f"[MEMORY_LOG] Python Helper (PID: {current_process.pid}): "
            f"{python_mem_mb:.2f} MB"
        )
    except psutil.NoSuchProcess:
        print("[MEMORY_LOG] Could not find the current Python process.")
        return

    # Find the Java KCL daemon process in a safe, cross-platform way
    found_java_daemon = False
    # Iterate over all running processes, fetching only the info we need
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Check if the process is a java process and if its command line
            # includes MultiLangDaemon to specifically target our KCL process.
            if (
                'java' in proc.info['name'].lower()
                and proc.info['cmdline']
                and 'MultiLangDaemon' in proc.info['cmdline']
            ):
                java_mem_mb = proc.memory_info().rss / (1024 * 1024)
                print(
                    f"[MEMORY_LOG] Java KCL Daemon (PID: {proc.info['pid']}): "
                    f"{java_mem_mb:.2f} MB"
                )
                found_java_daemon = True
                # Assuming only one KCL daemon, we can stop looking
                break
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # This can happen if the process dies while we are iterating
            pass

    if not found_java_daemon:
        print("[MEMORY_LOG] Java KCL Daemon process not found.")


def signal_handler(signum: int, frame: Optional[FrameType]) -> None:
    """Handle termination signals"""
    sig_name = signal.Signals(signum).name
    log_memory_usage()
    print(
        f"[KCL-HELPER] Received {sig_name}, shutting down "
        f"at {time.strftime('%Y-%m-%d %H:%M:%S')}",
        flush=True
    )
    sys.exit(0)


def get_kcl_dir_from_site_packages() -> str:
    possible_paths = [
        '/usr/local/lib/python3.12/site-packages/amazon_kclpy/jars',
        os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                '..',
                '..',
                'venv/lib/python3.12/site-packages/amazon_kclpy/jars'
            )
        )
    ]
    for path_attempt in possible_paths:
        if os.path.isdir(path_attempt):
            logger.info(f"Found KCL JARs directory at: {path_attempt}")
            return path_attempt

    try:
        from amazon_kclpy import kcl
        kcl_module_dir = os.path.dirname(os.path.abspath(kcl.__file__))
        jars_dir_via_module = os.path.join(kcl_module_dir, 'jars')
        if os.path.isdir(jars_dir_via_module):
            logger.info(
                f"Found KCL JARs directory via module path: {jars_dir_via_module}"
            )
            return jars_dir_via_module
    except ImportError:
        logger.warning(
            "Could not import amazon_kclpy.kcl to find JARs directory."
        )
        pass

    return None


def main() -> None:
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    TX_TYPE = os.getenv("TX_TYPE")
    properties_filename: str = None

    if TX_TYPE == "realtime":
        properties_filename = "kcl-config-realtime.properties"
    elif TX_TYPE == "reprocess":
        properties_filename = "kcl-config-reprocess.properties"
    else:
        raise ValueError(
            f"This {TX_TYPE} is unknown for tx-asset-cycle-vlx type."
        )

    config_file_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '..',
        'config', properties_filename
    ))

    logback_file_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '..',
        'config', 'logback.xml'
    ))

    if not os.path.isfile(config_file_path):
        logger.error(
            f"KCL properties file not found at: {config_file_path}"
        )
        sys.exit(1)

    if not os.path.isfile(logback_file_path):
        logger.error(
            f"logback file not found at: {logback_file_path}"
        )
        sys.exit(1)

    jars_base_dir = get_kcl_dir_from_site_packages()
    if not jars_base_dir:
        logger.error(
            "Could not locate KCL JARs directory. "
            "Please check installation or kclpy_helper.py paths."
        )
        sys.exit(1)

    classpath_separator = ';' if os.name == 'nt' else ':'
    classpath = classpath_separator.join(
        glob(os.path.join(jars_base_dir, '*.jar'))
    )

    # Main class for the Java KCL MultiLangDaemon
    kcl_main_class = 'software.amazon.kinesis.multilang.MultiLangDaemon'

    java_opts = os.getenv("JAVA_OPTS")
    command = ["java"]

    if java_opts:
        print(f"[KCL-HELPER] Using JAVA_OPTS: {java_opts}", flush=True)
    else:
        java_opts = '-Xms512m -Xmx768m -XX:+UseG1GC -XX:MaxGCPauseMillis=100'
        print(
            f"[KCL-HELPER] No JAVA_OPTS provided, using defaults: {java_opts}",
            flush=True
        )

    command.extend(java_opts.split())
    command.extend([
        f'-Dlogback.configurationFile={logback_file_path}',
        '-cp',
        classpath,
        kcl_main_class,
        '-p',
        config_file_path
    ])

    logger.info(
        f"Starting KCL MultiLangDaemon with command: {' '.join(command)}"
    )

    # Log just before starting the daemon
    print(
        "[KCL-HELPER] Launching KCL MultiLangDaemon for "
        f"stream: {os.getenv('KINESIS_STREAM_NAME', 'unknown')}",
        flush=True
    )
    print(
        f"[KCL-HELPER] Asset-Idle service type: {TX_TYPE}",
        flush=True
    )
    sys.stdout.flush()
    sys.stderr.flush()

    env = os.environ.copy()
    process = subprocess.Popen(command, env=env)

    try:
        process.wait()
        exit_code = process.returncode
    except KeyboardInterrupt:
        print(
            f"[KCL-HELPER] Interrupted, terminating "
            f"at {time.strftime('%Y-%m-%d %H:%M:%S')}",
            flush=True
        )
        process.terminate()
        process.wait()
        exit_code = 130  # Standard exit code for SIGINT

    # Log termination
    if exit_code != 0:
        log_memory_usage()
        print(
            f"[KCL-HELPER] Terminated with error code {exit_code} "
            f"at {time.strftime('%Y-%m-%d %H:%M:%S')}",
            flush=True
        )
        logger.error(f"KCL MultiLangDaemon exited with error code: {exit_code}")
        sys.exit(exit_code)
    else:
        print(
            "[KCL-HELPER] Terminated successfully "
            f"at {time.strftime('%Y-%m-%d %H:%M:%S')}",
            flush=True
        )
        logger.info("KCL MultiLangDaemon finished successfully.")


if __name__ == "__main__":
    main()
