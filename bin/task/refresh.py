import logging as log
from typing import Any

from common.config import config, initConfig
from common.logtools import initLogging

from db.dbtools import DbParams, dbConnect

PROFILE = "refresh"

def main() -> int:
    initConfig(PROFILE)
    initLogging(config.get("logLevel"))

    conn = dbConnect(DbParams.of(config["db"]))
    try:
        refresh(conn)
    finally:
        conn.close()

    return 0

def refresh(conn: Any) -> None:
    log.info("Refreshing materialized views")

    with conn.cursor() as curs:
        curs.execute("CALL refresh_mv();")

if __name__ == "__main__":
    exit(main())
