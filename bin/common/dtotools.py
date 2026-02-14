import dataclasses
from typing import Any, NamedTuple

def ofmethod[T: NamedTuple](cls: type[T]) -> type[T]:
    @classmethod
    def namedTupleOf[C: NamedTuple](cls: type[C], params: dict[str, Any], **kwargs) -> C:
        return __namedTupleOf(cls, params, **kwargs)

    @classmethod
    def dataClassOf[C](cls: type[C], params: dict[str, Any], **kwargs) -> C:
        return __dataClassOf(cls, params, **kwargs)

    if isNamedTuple(cls):
        setattr(cls, "of", namedTupleOf)
    elif dataclasses.is_dataclass(cls):
        setattr(cls, "of", dataClassOf)
    else:
        raise ValueError(f"Class should be a dataclass or a direct descendant of NamedTuple: {cls.__name__}")

    return cls

def isNamedTuple(cls: type) -> bool:
    return hasattr(cls, "_fields") and hasattr(cls, "_field_defaults")

def namedTupleOf[T: NamedTuple](cls: type[T], params: dict[str, Any], **kwargs) -> T:
    if not isNamedTuple(cls):
        raise ValueError(f"Class should be a direct descendant of NamedTuple: {cls.__name__}")
    return __namedTupleOf(cls, params, **kwargs)

def __namedTupleOf[T: NamedTuple](cls: type[T], params: dict[str, Any], **kwargs) -> T:
    params = {k: v for k, v in params.items() if k in cls._fields}
    params = cls._field_defaults | params | kwargs
    return cls(**params)

def dataClassOf[C](cls: type[C], params: dict[str, Any], **kwargs) -> C:
    if not dataclasses.is_dataclass(cls):
        raise ValueError(f"Class should be a dataclass: {cls.__name__}")
    return __dataClassOf(cls, params, **kwargs)

def __dataClassOf[C](cls: type[C], params: dict[str, Any], **kwargs) -> C:
    fieldNames = {f.name for f in dataclasses.fields(cls)}
    defaults = {f.name: f.default for f in dataclasses.fields(cls)}
    params = {k: v for k, v in params.items() if k in fieldNames}
    params = defaults | params | kwargs
    return cls(**params)
