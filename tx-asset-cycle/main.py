#!/usr/bin/env python3

import logging
import sys

from amazon_kclpy import kcl

from processing.extract_phase import RecordProcessor


def logging_setup() -> None:
    app_logger = logging.getLogger()
    handler = None
    if app_logger.hasHandlers():
        app_logger.handlers.clear()

    app_logger.setLevel(logging.ERROR)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.ERROR)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
        "(%(filename)s:%(lineno)d)"
    )
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)


logger = logging.getLogger(__name__)


def main() -> None:
    logging_setup()
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()


if __name__ == "__main__":
    main()
