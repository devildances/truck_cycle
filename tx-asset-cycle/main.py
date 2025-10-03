#!/usr/bin/env python3

import logging
import os
import sys

from amazon_kclpy import kcl

from processing.extract_phase import RecordProcessor


def logging_setup() -> None:
    app_logger = logging.getLogger()
    handler = None
    if app_logger.hasHandlers():
        app_logger.handlers.clear()

    log_level = os.getenv("PYTHON_LOG_LEVEL", "WARN").upper()
    numeric_level = getattr(logging, log_level, logging.WARN)

    app_logger.setLevel(numeric_level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(numeric_level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
        "(%(filename)s:%(lineno)d)"
    )
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)


logger = logging.getLogger(__name__)


def main() -> None:
    logging_setup()
    TX_TYPE = os.getenv("TX_TYPE", "REALTIME").upper()
    logger.warning(f" [NOTIFICATION] ASSET CYCLE SERVICE FOR {TX_TYPE} IS READY")
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()


if __name__ == "__main__":
    main()
