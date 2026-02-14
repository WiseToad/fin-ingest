from typing import Any
from enum import Enum
from datetime import date, time, datetime
from decimal import Decimal
from json import JSONEncoder

class JsonEncoderEx(JSONEncoder):
    def default(self, obj: Any) -> Any:
        serialize = getattr(obj, "serialize", None)
        if callable(serialize):
            return serialize()
        
        if isinstance(obj, Enum):
            return obj.name

        if isinstance(obj, Decimal):
            return str(obj)

        if isinstance(obj, datetime):
            return obj.isoformat(sep=" ")
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, time):
            return obj.isoformat()

        return super().default(obj)
