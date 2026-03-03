from typing import Any, Callable

def isEqual(v1: Any, v2: Any) -> bool:
    return v1 is v2 or v1 == v2

def apply[T, R](v: T | None, f: Callable[[T], R], **kwargs) -> R | None:
    return None if v is None else f(v, **kwargs)

def combine[T1, T2, R](v1: T1 | None, v2: T2 | None, f: Callable[[T1, T2], R]) -> R | None:
    return v1 if v2 is None else v2 if v1 is None else f(v1, v2)

def add[T1, T2, R](v1: T1 | None, v2: T2 | None) -> R:
    return combine(v1, v2, lambda v1, v2: v1 + v2)
