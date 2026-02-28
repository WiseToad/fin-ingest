import logging as log
import httpx
from typing import Any
from datetime import date, timedelta

from common.config import config, initConfig
from common.logtools import initLogging
from common.tools import getPeriodFromArgv, forEachSafely

from api.finamapi import FinamApi

import db.dbacc as dbacc
from db.dbtools import DbParams, dbConnect

class Ingestor:
    PROFILE = "acc-finam"
    BROKER = "FINAM"

    conn: Any
    finamApi: FinamApi

    def __init__(self):
        initConfig(self.PROFILE)
        initLogging(config.get("logLevel"))

    def run(self) -> bool:
        today = date.today()
        startDate, endDate = getPeriodFromArgv(today - timedelta(days=10), today)

        with open(config["tokenFile"], "r") as f:
            token = f.readline()

        self.conn = dbConnect(DbParams.of(config["acc-db"]))
        try:
            with httpx.Client(http2=True) as http:
                self.finamApi = FinamApi(http, token)
                return self.process(startDate, endDate)
        finally:
            self.conn.close()

    def process(self, startDate: date, endDate: date) -> bool:
        accountIds = self.finamApi.getAccountIds()
        return forEachSafely(accountIds, lambda accountId: self.processAccount(accountId, startDate, endDate))
    
    def processAccount(self, accountId: str, startDate: date, endDate: date) -> bool:
        log.info(f"Processing account: {accountId}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        with self.conn.cursor() as curs:
            id = dbacc.dbInsertAccount(curs, self.BROKER, accountId)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
