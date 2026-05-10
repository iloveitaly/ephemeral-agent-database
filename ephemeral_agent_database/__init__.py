import logging
import os
import sys

from .version import __version__

log = logging.getLogger(__name__)


def main():
    if "--version" in sys.argv or "-V" in sys.argv:
        print(f"ephemeral-agent-database version {__version__}")
        sys.exit(0)

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING"))
    log.info("Hello, Logs!")
