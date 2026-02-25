import logging as log
import httpx
import json
import re
from typing import Any
from typing import NamedTuple
from datetime import date, datetime, timedelta
from decimal import Decimal
from collections import defaultdict

from common.config import config, initConfig
from common.logtools import initLogging
from common.dtotools import ofmethod
from common.tools import getPeriodFromArgv, forEachSafely, toIterable
from common.datetools import dateToDt, MOSCOW_TZ

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

class SearchParams(NamedTuple):
    mic: str
    tickers: list[str] = None
    patterns: list[str] = None

@ofmethod
class Asset(NamedTuple):
    mic: str
    ticker: str
    name: str
    symbol: str

class Bar(NamedTuple):
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

class Ingestor:
    PROFILE = "finam"

    API_BASE_URL = "https://api.finam.ru/v1"

    TIME_FRAME = "TIME_FRAME_D"

    conn: Any
    http: httpx.Client

    jwtToken: str = None

    def __init__(self):
        initConfig(self.PROFILE)
        initLogging(config.get("logLevel"))

    def run(self) -> bool:
        today = date.today()
        startDate, endDate = getPeriodFromArgv(today - timedelta(days=10), today)

        self.conn = dbConnect(DbParams.of(config["db"]))
        try:
            with httpx.Client(http2=True) as http:
                self.http = http
                return self.process(startDate, endDate)
        finally:
            self.conn.close()

    def process(self, startDate: date, endDate: date) -> bool:
        searchParams = [
            SearchParams(
                mic=s["mic"],
                patterns=toIterable(s.get("patterns")),
                tickers=toIterable(s.get("tickers"))) 
            for s in config.get("assets", [])
        ]
        assets = self.findAssets(searchParams)
        return forEachSafely(assets, lambda asset: self.processAsset(asset, startDate, endDate))

    def processAsset(self, asset: Asset, startDate: date, endDate: date) -> None:
        log.info(f"Processing: {asset.symbol}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        bars = self.fetchBars(asset.symbol, startDate, endDate, self.TIME_FRAME)
        if not bars:
            log.warning("No bars retrieved")
            return
        
        key = lambda b: b.timestamp
        count, start, end = len(bars), min(bars, key=key), max(bars, key=key)
        log.info(f"Fetched {count} bars, period: {start.timestamp.isoformat()} to {end.timestamp.isoformat()}")

        if any(b.volume != int(b.volume) for b in bars):
            raise ValueError("Fractional volumes do not supported")

        self.dbLoad(asset, bars)

    def findAssets(self, searchParams: list[SearchParams]) -> list[Asset]:
        assets = defaultdict(dict)
        for a in self.fetchAssets():
            assets[a.mic][a.ticker] = a
            
        searchTickers = [s for s in searchParams if s.tickers]
        tickerCount = sum(len(s.tickers) for s in searchTickers)

        foundAssets = {
            a.symbol: a
            for s in searchTickers
            for t, a in assets[s.mic].items()
            if t in s.tickers
        }

        assetCount = len(foundAssets)
        if assetCount != tickerCount:
            log.warning(f"Found {assetCount} assets, but {tickerCount} were requested")

        for s in searchParams:
            if not s.patterns:
                continue
            for pattern in s.patterns:
                p = re.compile(pattern)
                a = {a.symbol: a for a in assets[s.mic].values() if p.search(a.name)}
                if not a:
                    log.warning(f"Found no assets for {s.mic} by pattern: {pattern}")
                foundAssets.update(a)

        return foundAssets.values()

    def fetchAssets(self) -> list[Asset]:
        data = self.callApi("assets", None)
        return [Asset.of(a) for a in data["assets"]]

    def fetchBars(self, symbol: str, startDate: date, endDate: date, timeFrame: str) -> list[Bar]:
        url = f"instruments/{symbol}/bars"
        
        startDt = dateToDt(startDate, MOSCOW_TZ)
        endDt = dateToDt(endDate + timedelta(days=1), MOSCOW_TZ)
        params = {
            "interval.start_time": startDt.isoformat(),
            "interval.end_time": endDt.isoformat(),
            "timeframe": timeFrame
        }

        data = self.callApi(url, params)

        return [
            Bar(timestamp=datetime.fromisoformat(b["timestamp"]),
                open=Decimal(b["open"]["value"]),
                high=Decimal(b["high"]["value"]),
                low=Decimal(b["low"]["value"]),
                close=Decimal(b["close"]["value"]),
                volume=Decimal(b["volume"]["value"]))
            for b in data["bars"]
        ]

    def callApi(self, url: str, params: dict[str, Any]) -> Any:
        url = f"{self.API_BASE_URL}/{url}"
        response = self.tryWithAuth(url, params)
        response.raise_for_status()
        return json.loads(response.text)

    def tryWithAuth(self, url: str, params: dict[str, Any]) -> httpx.Response:
        response = self.callWithAuth(url, params)

        if response.status_code == 401:
            log.debug(f"Unauthorized, trying to recover")
            return self.callWithAuth(url, params, True)

        if response.status_code == 500:
            try:
                data = json.loads(response.text)
                code = data["code"]
            except (json.JSONDecodeError, KeyError) as e:
                log.warning(f"Failed to recover after HTTP 500: {e}")
                return response

            if code == 13:
                log.debug(f"Token error: {data.get("message")}")
                return self.callWithAuth(url, params, True)

        return response

    def callWithAuth(self, url: str, params: dict[str, Any], renewToken: bool = False) -> httpx.Response:
        if not self.jwtToken or renewToken:
            self.updateToken()

        return self.http.get(url, params=params, headers={"Authorization": self.jwtToken})

    def updateToken(self) -> None:
        log.debug("Updating token")

        tokenFile = config["tokenFile"]
        with open(tokenFile, "r") as f:
            token = f.readline()

        url = f"{self.API_BASE_URL}/sessions"
        response = self.http.post(url, json={"secret": token})
        response.raise_for_status()
        
        data = json.loads(response.text)
        self.jwtToken = data["token"]

    def dbLoad(self, asset: Asset, bars: list[Bar]) -> None:
        log.info(f"Loading into DB: {asset.mic} {asset.ticker}")

        with self.conn.cursor() as curs:
            assetId = dbfin.dbInsertAsset(curs, asset.mic, asset.ticker, asset.name, update=True)
            valueCols = (dbfin.Trades.O, dbfin.Trades.H, dbfin.Trades.L, dbfin.Trades.C, dbfin.Trades.V)
            dbfin.dbInsertTrades(curs, assetId, bars, valueCols, dbfin.AggType.DAILY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
