import sys
import logging as log
from typing import Any, Iterable, Callable
from datetime import date, timedelta

def getDateFromArgv() -> date:
    if len(sys.argv) > 2:
        log.warning(f"Extra command line args ignored: {sys.argv[2:]}")

    if len(sys.argv) >= 2:
        return date.fromisoformat(sys.argv[1])

    return date.today()

def getPeriodFromArgv(startDiff: int | Callable[[date], date] = None) -> tuple[date, date]:
    if len(sys.argv) > 3:
        log.warning(f"Extra command line args ignored: {sys.argv[3:]}")

    if len(sys.argv) >= 3:
        return date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])
    
    if len(sys.argv) == 2:
        d = date.fromisoformat(sys.argv[1])
        return d, d

    d = date.today()
    if startDiff is None:
        return d, d
    if isinstance(startDiff, int):
        return (d - timedelta(days=startDiff)), d
    if isinstance(startDiff, Callable):
        return startDiff(d), d
    
    raise ValueError(f"Invalid type of default period length: {startDiff}")

def forEachSafely[T](items: Iterable[T], process: Callable[[T], bool | None]) -> bool:
    success = True
    for item in items:
        try:
            ret = process(item)
            if ret is not None:
                success = success and ret
        except Exception:
            log.exception(f"Failed to process: {item}")
            success = False
    
    return success

def toIterable(value: Any, scalars: type | Iterable[type] = None) -> Iterable:
    if value is None:
        return ()
    
    if scalars is None:
        scalars = str
        
    if isinstance(value, scalars) or not isinstance(value, Iterable):
        return (value,)

    return value
