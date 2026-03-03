import logging as log
import httpx
from typing import Any
from typing import NamedTuple
from datetime import date, datetime, timedelta
from decimal import Decimal
from operator import attrgetter

from common.config import config, initConfig
from common.logtools import initLogging
from common.dtotools import ofmethod
from common.datetools import dateToDt, MOSCOW_TZ
from common.tools import getPeriodFromArgv, forEachSafely
from common.tools import cvtNoneable

from api.finamapi import FinamApi

import db.dbacc as dbacc
from db.dbtools import DbParams, ColumnDef 
from db.dbtools import dbConnect, dbTempTable, dbLoadData, dbMerge

class Account(NamedTuple):
    account_id: str
    open_account_date: datetime

class Trade(NamedTuple):
    id: str
    symbol: str
    price: Decimal
    size: Decimal
    side: str
    timestamp: datetime
    comment: str

class Transaction(NamedTuple):
    id: str
    category: str
    timestamp: datetime
    symbol: str
    currencyCode: str
    units: Decimal
    changeQty: Decimal
    transactionCategory: str
    transactionName: str

class OpImport(NamedTuple):
    code: str
    transDt: datetime
    opType: str
    symbol: str
    quantity: int
    amount: Decimal
    cur: str
    comment: str

@ofmethod
class Asset(NamedTuple):
    mic: str
    ticker: str
    isin: str
    name: str
    quote_currency: str

class Ingestor:
    PROFILE = "acc-finam"
    BROKER = "FINAM"

    TRADE_OP_TYPES = {
        "SIDE_BUY": "BUY",
        "SIDE_SELL": "SELL"
    }
    TRANS_OP_TYPES = {
        "DEPOSIT": "DEPOSIT",
        "WITHDRAW": "WITHDRAW",
        "TRANSFER": "TRANSFER",
        "INCOME": "INCOME",
        "OUTCOMES": "OUTCOMES",
        "COMMISSION": "COMMISSION",
        "TAX": "TAX"
    }

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
        finamAccIds = self.finamApi.getAccountIds()
        return forEachSafely(finamAccIds, lambda finamAccId: self.processAccount(finamAccId, startDate, endDate))
    
    def processAccount(self, finamAccId: str, startDate: date, endDate: date) -> bool:
        log.info(f"Processing account: {finamAccId}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        startDt = dateToDt(startDate, MOSCOW_TZ)
        endDt = dateToDt(endDate, MOSCOW_TZ) + timedelta(days=1)

        account = self.fetchAccount(finamAccId)
        if endDt < account.open_account_date:
            # Further requests will complain with HTTP 400 without this check 
            log.info(f"Period if beyond account open date, skipping")
            return

        trades = self.fetchTrades(finamAccId, startDt, endDt)
        trans = self.fetchTrans(finamAccId, startDt, endDt)
        log.info(f"Found {len(trades)} trades and {len(trans)} non-trading transactions")

        with self.conn.cursor() as curs:
            with self.conn:
                accountId = dbacc.dbInsertAccount(curs, self.BROKER, finamAccId)

            with self.conn:
                symbols = {t.symbol for t in trades if t.symbol} | {t.symbol for t in trans if t.symbol}
                symbols = self.filterNewSymbols(curs, symbols)
                assets = [self.fetchAsset(finamAccId, s) for s in symbols]
                log.info(f"Found {len(assets)} new assets")

                if assets:
                    self.dbLoadAssets(curs, assets)

            with self.conn:
                if trades:
                    self.dbLoadTrades(curs, accountId, trades)

                if trans:
                    self.dbLoadTrans(curs, accountId, trans)

    def fetchAccount(self, finamAccId: str) -> Account:
        url = f"accounts/{finamAccId}"
        data = self.finamApi.get(url, {})

        return Account(
            account_id=data["account_id"],
            open_account_date=datetime.fromisoformat(data["open_account_date"])
        )

    def fetchTrades(self, finamAccId: str, startDt: datetime, endDt: datetime) -> list[Trade]:
        url = f"accounts/{finamAccId}/trades"
        params = {
            "interval.start_time": startDt.isoformat(),
            "interval.end_time": endDt.isoformat()
        }
        data = self.finamApi.get(url, params)

        return [
            Trade(
                id=t["trade_id"],
                symbol=t["symbol"],
                price=Decimal(t["price"]["value"]),
                size=Decimal(t["size"]["value"]),
                side=t["side"],
                timestamp=datetime.fromisoformat(t["timestamp"]),
                comment=t["comment"])
            for t in data["trades"]
        ]
    
    def fetchTrans(self, finamAccId: str, startDt: datetime, endDt: datetime) -> list[Transaction]:
        url = f"accounts/{finamAccId}/transactions"
        params = {
            "interval.start_time": startDt.isoformat(),
            "interval.end_time": endDt.isoformat()
        }
        data = self.finamApi.get(url, params)

        return [
            Transaction(
                id=t["id"],
                category=t["category"],
                timestamp=datetime.fromisoformat(t["timestamp"]),
                symbol=t["symbol"],
                currencyCode=t["change"]["currency_code"],
                units=Decimal(t["change"]["units"]),
                changeQty=cvtNoneable(t.get("change_qty", {}).get("value"), Decimal),
                transactionCategory=t["transaction_category"],
                transactionName=t["transaction_name"])
            for t in data["transactions"]
        ]

    def filterNewSymbols(self, curs, symbols: list[str]) -> list[str]:
        tempTable = "symbols"
        cols = (dbacc.Assets.TICKER, dbacc.Assets.MARKET)
        data = [s.split("@", maxsplit=1) for s in symbols]

        dbTempTable(curs, tempTable, cols)
        dbLoadData(curs, tempTable, data, cols)

        curs.execute(
            f"SELECT DISTINCT t.market, t.ticker FROM {tempTable} AS t "
            "LEFT JOIN assets AS a ON a.market = t.market AND a.ticker = t.ticker "
            "WHERE a.id IS NULL;"
        )
        data = curs.fetchall()

        return [f"{ticker}@{market}" for market, ticker in data]

    def fetchAsset(self, finamAccId: str, symbol: str) -> Asset:
        url = f"assets/{symbol}"
        params = {"account_id": finamAccId}

        try:
            data = self.finamApi.get(url, params)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                raise

            # to circumvent the case if asset is archived
            ticker, mic = symbol.split("@", maxsplit=1)
            return Asset(mic, ticker, isin=None, name=f"Not Found: {symbol}", quote_currency="???")

        if not data.get("isin"):
            data["isin"] = None

        return Asset.of(data)
    
    def dbLoadAssets(self, curs, assets: list[Asset]) -> None:
        symbols = ", ".join(f"{a.ticker}@{a.mic}" for a in assets)
        log.info(f"Loading into DB: {symbols}")

        cols = (dbacc.Assets.MARKET, dbacc.Assets.TICKER, dbacc.Assets.ISIN, dbacc.Assets.NAME, dbacc.Assets.CUR)
        dbLoadData(curs, "assets", assets, cols)

    def dbLoadTrades(self, curs, accountId: int, trades: list[Trade]) -> None:
        log.info(f"Loading trades into DB")

        self.validateQuantity(trades, "size")

        opImports = [
            OpImport(
                code=t.id,
                transDt=t.timestamp,
                opType=self.TRADE_OP_TYPES[t.side],
                symbol=t.symbol,
                quantity=int(t.size),
                amount=(t.price * t.size),
                cur=None,
                comment=t.comment)
            for t in trades
        ]

        self.dbLoadOps(curs, accountId, opImports, "trades")

    def dbLoadTrans(self, curs, accountId: int, trans: list[Transaction]) -> None:
        log.info(f"Loading non-trading transactions into DB")

        self.validateQuantity(trans, "changeQty")

        invalid = [t for t in trans if t.category != t.transactionCategory]
        if invalid:
            raise Exception(f"Non-trading transactions have mismatching categories: {invalid}")

        opImports = [
            OpImport(
                code=t.id,
                transDt=t.timestamp,
                opType=self.TRANS_OP_TYPES[t.category],
                symbol=t.symbol,
                quantity=None,
                amount=t.units,
                cur=t.currencyCode,
                comment=t.transactionName)
            for t in trans
        ]

        self.dbLoadOps(curs, accountId, opImports, "trans")

    def validateQuantity(self, items: list[Any], quantityAttr: str):
        getQuantity = attrgetter(quantityAttr)
        isInvalid = lambda n: n is not None and n % 1 != 0

        invalid = [i for i in items if isInvalid(getQuantity(i))]
        if invalid:
            raise Exception(f"Fractional quantity not supported: {invalid}")

    def dbLoadOps(self, curs, accountId: int, opImports: list[OpImport], impName: str) -> None:
        impTable = f"{impName}_imp"
        cols = (
            dbacc.Ops.CODE,
            dbacc.Ops.TRANS_DT,
            dbacc.Ops.OP_TYPE,
            ColumnDef("symbol"),
            dbacc.Ops.QUANTITY,
            dbacc.Ops.AMOUNT,
            dbacc.Ops.CUR,
            dbacc.Ops.COMMENT
        )
        dbTempTable(curs, impTable, cols)
        dbLoadData(curs, impTable, opImports, cols)

        stagingTable = f"{impName}_staging"
        cols = (
            dbacc.Ops.BROKER,
            dbacc.Ops.CODE,
            dbacc.Ops.ACCOUNT_ID,
            dbacc.Ops.TRANS_DT,
            dbacc.Ops.OP_TYPE,
            dbacc.Ops.ASSET_ID,
            dbacc.Ops.QUANTITY,
            dbacc.Ops.AMOUNT,
            dbacc.Ops.CUR,
            dbacc.Ops.COMMENT
        )
        dbTempTable(curs, stagingTable, cols)

        curs.execute(
            f"INSERT INTO {stagingTable} "
            f"SELECT '{self.BROKER}' AS broker, "
                "t.code, "
                f"{accountId} AS account_id, "
                "t.trans_dt, "
                "t.op_type, "
                "a.id AS asset_id, "
                "t.quantity, "
                "t.amount, "
                "t.cur, "
                "t.comment "
            f"FROM {impTable} AS t "
            "LEFT JOIN assets AS a "
                "ON a.market = SPLIT_PART(t.symbol, '@', 2) "
                    "AND a.ticker = SPLIT_PART(t.symbol, '@', 1);"
        )

        dbMerge(curs, "ops", stagingTable, on=("broker", "code"), cols=cols)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
