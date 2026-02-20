import logging as log
from typing import Any, Generator
from typing import NamedTuple
from datetime import date, datetime
from decimal import Decimal

from common.config import config, initConfig
from common.logtools import initLogging
from common.tools import getDateFromArgv, forEachSafely, toIterable
from common.datetools import dateToDt, MOSCOW_TZ

from api.selentools import initWebDriver, callApiNoF5

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

class RateValues(NamedTuple):
    dt: datetime
    rateBuy: Decimal
    rateSell: Decimal
    unit: str = None

class Ingestor:
    PROFILE="sber"
    MARKET="SBER"

    API_URL = "https://www.sberbank.ru/proxy/services/rates/public/v2/historyIngots"
    API_PARAMS = {"segType": "TRADITIONAL", "id": 38}

    VALID_MSGS = [
        "Отсутствие курсов валют"
    ]

    WRAPPER_ABBR = {
        "STANDARD": "STD",
        "HIGH_QUALITY": "HQ"
    }

    RATE_TYPE = "PMR-1"

    conn: Any
    driver: Any

    def __init__(self):
        initConfig(self.PROFILE)
        initLogging(config.get("logLevel"))

    def run(self) -> bool:
        d = getDateFromArgv()

        self.conn = dbConnect(DbParams.of(config["db"]))
        try:
            self.driver = initWebDriver()
            try:
                return self.process(d)
            finally:
                self.driver.quit()
        finally:
            self.conn.close()

    def process(self, d: date) -> bool:
        isoCodes = toIterable(config.get("isoCodes", []))
        return forEachSafely(isoCodes, lambda isoCode: self.processIsoCode(isoCode, self.RATE_TYPE, d))

    def processIsoCode(self, isoCode: str, rateType: str, d: date) -> None:
        log.info(f"Processing: {isoCode} {rateType}, date: {d.isoformat()}")

        rateValues = self.fetchValues(isoCode, rateType, d)
        if not rateValues:
            log.warning(f"No values retrieved")
            return

        self.dbLoad(isoCode, rateType, rateValues)

    def fetchValues(self, isoCode: str, rateType: str, d: date) -> dict[str, list[RateValues]]:
        dt = dateToDt(d, MOSCOW_TZ)
        ts = int(dt.timestamp()) * 1000

        params = self.API_PARAMS | {"isoCode": isoCode, "rateType": rateType, "date": ts}
        data = callApiNoF5(self.driver, self.API_URL, params)
        self.validateResponse(data)

        return self.parseApiV2(isoCode, rateType, data)

    def validateResponse(self, data: dict[str, Any]) -> None:
        message = data.get("message")
        if message is not None and message not in self.VALID_MSGS:
            raise ValueError(f"Message from response: {message}")

    # used for physical metal trading tariffs (PMR-1)
    def parseApiV2(self, isoCode: str, rateType: str, data: dict[str, Any]) -> dict[str, list[RateValues]]:
        rateValues = {}
        for dt, rangeList in self.preParse(isoCode, rateType, data).items():
            dt = datetime.fromtimestamp(int(dt) / 1000)
            rangeList = (r for r in rangeList if r["condition"] == "EXCELLENT")
            for range in rangeList:
                mass = str(range["mass"])
                wrapper = range["wrapper"]
                wrapper = self.WRAPPER_ABBR.get(wrapper, wrapper[0:4])
                subCode = f"{wrapper}-{mass}"
                values = RateValues(dt, Decimal(range["rateBuy"]), Decimal(range["rateSell"]), mass)
                rateValues.setdefault(subCode, []).append(values)
        return rateValues

    # used for metal account rates (PMR-3 - PMR-7)
    def parseApiV3(self, isoCode: str, rateType: str, data: dict[str, Any]) -> dict[str, list[RateValues]]:
        rateValues = {}
        for dt, rangeList in self.preParse(isoCode, rateType, data).items():
            dt = datetime.fromtimestamp(int(dt) / 1000)
            for range in rangeList:
                subCode = f"GT{str(range["rangeAmountBottom"])}"
                values = RateValues(dt, Decimal(range["rateBuy"]), Decimal(range["rateSell"]))
                rateValues.setdefault(subCode, []).append(values)
        return rateValues

    def preParse(self, isoCode: str, rateType: str, data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        historyRates: dict[str, Any] = data.get("historyRates", {})
        values: Generator[dict[str, Any]] = (v[rateType][isoCode] for v in historyRates.values())
        return {k: v["rangeList"] for d in values for k, v in d.items() if v["lotSize"] == 1}

    def dbLoad(self, isoCode: str, rateType: str, rateValues: dict[str, list[RateValues]]) -> None:
        for subCode, values in rateValues.items():
            prefix = f"{isoCode}-{rateType}-{subCode}"

            data = [(v.dt, v.rateBuy, v.unit) for v in values]
            self.dbLoadAsset(f"{prefix}-BUY", data)

            data = [(v.dt, v.rateSell, v.unit) for v in values]
            self.dbLoadAsset(f"{prefix}-SELL", data)

    def dbLoadAsset(self, assetCode: str, data: list[tuple[datetime, Decimal, str]]) -> None:
        log.info(f"Loading into DB: {self.MARKET} {assetCode}")

        with self.conn.cursor() as curs:
            assetId = dbfin.dbInsertAsset(curs, self.MARKET, assetCode)
            valueCols = (dbfin.Trades.C, dbfin.Trades.UNIT)
            dbfin.dbInsertTrades(curs, assetId, data, valueCols, dbfin.AggType.INTRADAY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
