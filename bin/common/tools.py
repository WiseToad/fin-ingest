import sys
import logging as log
from typing import Any, Iterable, Callable
from datetime import date, timedelta

def getDateFromArgv(defaultDate: date = None) -> date:
    if len(sys.argv) > 2:
        log.warning(f"Extra command line args ignored: {sys.argv[2:]}")

    if len(sys.argv) >= 2:
        return date.fromisoformat(sys.argv[1])

    if defaultDate is None:
        return date.today()

    return defaultDate

def getPeriodFromArgv(defaultStart: date = None, defaultEnd: date = None) -> tuple[date, date]:
    if len(sys.argv) > 3:
        log.warning(f"Extra command line args ignored: {sys.argv[3:]}")

    if len(sys.argv) >= 3:
        return date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])
    
    if len(sys.argv) == 2:
        d = date.fromisoformat(sys.argv[1])
        return d, d

    if defaultStart is None:
        defaultStart = date.today()
    if defaultEnd is None:
        defaultEnd = defaultStart

    return defaultStart, defaultEnd

def forEachSafely[T](items: Iterable[T], process: Callable[[T], bool | None], breakOnFailure: bool = False) -> bool:
    success = True
    for item in items:
        try:
            ret = process(item)
            if ret is not None:
                success = success and ret
        except Exception:
            log.exception(f"Failed to process: {item}")
            success = False
    
        if breakOnFailure and not success:
            break

    return success

def toIterable(value: Any, scalars: type | Iterable[type] = None) -> Iterable:
    if value is None:
        return ()
    
    if scalars is None:
        scalars = str
        
    if isinstance(value, scalars) or not isinstance(value, Iterable):
        return (value,)

    return value
