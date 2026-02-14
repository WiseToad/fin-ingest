import logging
from typing import Any

def initLogging(level: Any) -> None:
    logging.basicConfig(level=__normalizeLevel(level), format="%(levelname)-5s %(message)s")

    logging.addLevelName(logging.WARNING, 'WARN')
    logging.addLevelName(logging.CRITICAL, 'CRIT')

def __normalizeLevel(level: Any) -> int:
    if level is None:
        return logging.INFO
    if isinstance(level, str):
        level = logging.getLevelName(level)
    if isinstance(level, int):
        return level
    raise ValueError(f"Invalid logging level")
