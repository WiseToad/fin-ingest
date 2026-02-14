import logging as log
from typing import Any
from typing import NamedTuple
from datetime import datetime, date
from decimal import Decimal

from common.config import config, initConfig
from common.logtools import initLogging
from common.tools import getPeriodFromArgv, forEachSafely, toIterable

from api.soapclient import callSoap

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

class RateValue(NamedTuple):
    dt: date
    rate: Decimal

class Ingestor:
    PROFILE = "cbr"
    MARKET = "CBR"

    API_URL = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"

    conn: Any

    def __init__(self):
        initConfig(self.PROFILE)
        initLogging(config.get("logLevel"))

    def run(self) -> bool:
        startDate, endDate = getPeriodFromArgv(10)

        self.conn = dbConnect(DbParams.of(config["db"]))
        try:
            return self.process(startDate, endDate)
        finally:
            self.conn.close()

    def process(self, startDate: date, endDate: date) -> bool:
        curCodes = toIterable(config.get("curCodes", []))
        return forEachSafely(curCodes, lambda curCode: self.processCurCode(curCode, startDate, endDate))

    def processCurCode(self, curCode: str, startDate: date, endDate: date) -> None:
        log.info(f"Processing: {curCode}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        rateValues = self.fetchValues(curCode, startDate, endDate)
        if not rateValues:
            log.warning(f"No values retrieved")
            return

        key = lambda v: v.dt
        count, start, end = len(rateValues), min(rateValues, key=key), max(rateValues, key=key)
        log.info(f"Fetched {count} values, period: {start.dt.isoformat()} to {end.dt.isoformat()}")

        self.dbLoad(curCode, rateValues)

    def fetchValues(self, curCode: str, startDate: date, endDate: date) -> list[RateValue]:
        data = callSoap(self.API_URL, "GetCursDynamicXML", FromDate=startDate.isoformat(), ToDate=endDate.isoformat(), ValutaCode=curCode)
        data = data.GetCursDynamicXMLResponse.GetCursDynamicXMLResult.ValuteData

        try:
            values = data.ValuteCursDynamic
        except AttributeError:
            values = []

        return [
            RateValue(
                datetime.fromisoformat(v.CursDate.cdata).date(),
                Decimal(v.VunitRate.cdata))
            for v in values
        ]

    def dbLoad(self, curCode: str, values: list[RateValue]) -> None:
        log.info(f"Loading into DB: {self.MARKET} {curCode}")

        with self.conn.cursor() as curs:
            assetId = dbfin.dbInsertAsset(curs, self.MARKET, curCode)
            dbfin.dbInsertTrades(curs, assetId, values, dbfin.Trades.C, dbfin.AggType.DAILY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
