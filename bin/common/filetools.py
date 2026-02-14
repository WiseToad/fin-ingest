import os
import logging as log
import json
from typing import Any, Callable, IO
from datetime import datetime
from common.jsontools import JsonEncoderEx

def fileNameWithTs(fileName: str) -> str:
    name, ext = os.path.splitext(fileName)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{name}.{ts}{ext}"

def saveText(fileName: str, data: str) -> None:
    saveData(fileName, lambda f: f.write(data))

def saveJson(fileName: str, data: Any) -> None:
    saveData(fileName, lambda f: json.dump(data, f, indent=2, cls=JsonEncoderEx))

def saveData(fileName: str, saver: Callable[[IO], None]) -> None:
    dir = os.path.dirname(fileName)
    if dir:
        os.makedirs(dir, exist_ok=True)
        
    with open(fileName, "w") as f:
        saver(f)
    log.info(f"Data saved: {fileName}")
