import logging as log
from typing import Any
from typing import NamedTuple
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict

from common.config import config, initConfig
from common.logtools import initLogging
from common.tools import getPeriodFromArgv, forEachSafely, toIterable

from api.soapclient import callSoap

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

class Value(NamedTuple):
    dt: date
    value: Decimal

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
        proc = (
            lambda: self.processCurRates(startDate, endDate),
            lambda: self.processMetalPrices(startDate, endDate)
        )
        return forEachSafely(proc, lambda p: p())

    def processCurRates(self, startDate: date, endDate: date) -> bool:
        # CBR's internal currency codes
        # See also: https://www.cbr.ru/dailyinfowebserv/dailyinfo.asmx?op=EnumValutesXML
        curCodes = toIterable(config.get("curCodes", []))
        return forEachSafely(curCodes, lambda curCode: self.processRatesForCur(curCode, startDate, endDate))

    def processRatesForCur(self, curCode: str, startDate: date, endDate: date) -> None:
        log.info(f"Processing currency: {curCode}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        rateValues = self.fetchCurRates(curCode, startDate, endDate)
        if not rateValues:
            log.warning(f"No currency rates retrieved")
            return

        key = lambda v: v.dt
        count, start, end = len(rateValues), min(rateValues, key=key), max(rateValues, key=key)
        log.info(f"Fetched {count} currency rates, period: {start.dt.isoformat()} to {end.dt.isoformat()}")

        self.dbLoad(curCode, rateValues)

    def fetchCurRates(self, curCode: str, startDate: date, endDate: date) -> list[Value]:
        data = callSoap(self.API_URL, "GetCursDynamicXML", FromDate=startDate.isoformat(), ToDate=endDate.isoformat(), ValutaCode=curCode)
        data = data.GetCursDynamicXMLResponse.GetCursDynamicXMLResult.ValuteData

        try:
            values = data.ValuteCursDynamic
        except AttributeError:
            values = []

        return [
            Value(
                datetime.fromisoformat(v.CursDate.cdata).date(),
                Decimal(v.VunitRate.cdata))
            for v in values
        ]

    def processMetalPrices(self, startDate: date, endDate: date) -> bool:
        # CBR's internal metal codes
        # See also: https://www.cbr.ru/development/DWS - DragMetDynamic
        metalCodes = set(toIterable(config.get("metalCodes", [])))

        log.info(f"Processing metal codes: {metalCodes}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        prices = self.fetchMetalPrices(startDate, endDate)
        prices = {k: v for k, v in prices.items() if k in metalCodes}

        if not prices:
            log.warning(f"No metal prices retrieved")
            return

        metalCount, priceCount = len(prices), sum(len(v) for v in prices.values())
        log.info(f"Fetched {priceCount} prices for {metalCount} metals")

        return forEachSafely(metalCodes, lambda metalCode: self.dbLoad(f"METAL{metalCode}", prices[metalCode]))

    def fetchMetalPrices(self, startDate: date, endDate: date) -> dict[int, Value]:
        data = callSoap(self.API_URL, "DragMetDynamicXML", fromDate=startDate.isoformat(), ToDate=endDate.isoformat())
        data = data.DragMetDynamicXMLResponse.DragMetDynamicXMLResult.DragMetall

        try:
            values = data.DrgMet
        except AttributeError:
            values = []

        prices = defaultdict(list)
        for v in values:
            prices[int(v.CodMet.cdata)].append(Value(
                datetime.fromisoformat(v.DateMet.cdata).date(),
                Decimal(v.price.cdata)))
        return prices

    def dbLoad(self, code: str, values: list[Value]) -> None:
        log.info(f"Loading into DB: {self.MARKET} {code}")

        with self.conn.cursor() as curs:
            assetId = dbfin.dbInsertAsset(curs, self.MARKET, code)
            dbfin.dbInsertTrades(curs, assetId, values, dbfin.Trades.C, dbfin.AggType.DAILY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
