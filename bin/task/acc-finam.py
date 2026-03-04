import logging as log
import httpx
import operator, itertools, functools
from typing import Any
from typing import NamedTuple
from datetime import date, datetime, timedelta
from decimal import Decimal

import common.noneable as noneable
from common.config import config, initConfig
from common.logtools import initLogging
from common.dtotools import ofmethod
from common.datetools import dateToDt, MOSCOW_TZ
from common.tools import getPeriodFromArgv, forEachSafely

from api.finamapi import FinamApi

import db.dbacc as dbacc
from db.dbtools import DbParams, ColumnDef 
from db.dbtools import dbConnect, dbTempTable, dbLoadData, dbMerge

class Account(NamedTuple):
    account_id: str
    open_account_date: datetime

class Op(NamedTuple):
    code: str
    transDt: datetime
    opType: str
    symbol: str
    quantity: Decimal
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
        "SIDE_SELL": "SELL",
        "SIDE_UNSPECIFIED": "UNSPECIFIED"
    }
    TRANS_OP_TYPES = {
        "OTHERS": "Прочее",
        "DEPOSIT": "Ввод ДС",
        "WITHDRAW": "Вывод ДС",
        "INCOME": "Доход",
        "COMMISSION": "Комиссия",
        "TAX": "Налог",
        "INHERITANCE": "Наследство",
        "TRANSFER": "Перевод ДС",
        "CONTRACT_TERMINATION": "Расторжение договора",
        "OUTCOMES": "Расходы",
        "FINE": "Штраф",
        "LOAN": "Займ"
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
        accountCodes = self.finamApi.getAccountIds()
        return forEachSafely(accountCodes, lambda accountCode: self.processAccount(accountCode, startDate, endDate))
    
    def processAccount(self, accountCode: str, startDate: date, endDate: date) -> bool:
        log.info(f"Processing account: {accountCode}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        startDt = dateToDt(startDate, MOSCOW_TZ)
        endDt = dateToDt(endDate, MOSCOW_TZ) + timedelta(days=1)

        account = self.fetchAccount(accountCode)
        if endDt < account.open_account_date:
            # Further requests will complain with HTTP 400 without this check 
            log.info(f"Period if beyond account open date, skipping")
            return

        trades = self.fetchTrades(accountCode, startDt, endDt)
        trans = self.fetchTrans(accountCode, startDt, endDt)
        log.info(f"Found {len(trades)} trades and {len(trans)} non-trading transactions")

        ops = trades + trans
        self.validateQuantity(ops)
        self.fixOpCodes(accountCode, ops)

        with self.conn.cursor() as curs:
            with self.conn:
                accountId = dbacc.dbInsertAccount(curs, self.BROKER, accountCode)

            with self.conn:
                symbols = {op.symbol for op in ops if op.symbol}
                symbols = self.filterNewSymbols(curs, symbols)
                assets = [self.fetchAsset(accountCode, s) for s in symbols]
                log.info(f"Found {len(assets)} new assets")

                if assets:
                    self.dbLoadAssets(curs, assets)

            with self.conn:
                if ops:
                    self.dbLoadOps(curs, accountId, ops)

                self.dbLinkOps(curs)

    def fetchAccount(self, accountCode: str) -> Account:
        url = f"accounts/{accountCode}"
        data = self.finamApi.get(url, {})

        return Account(
            account_id=data["account_id"],
            open_account_date=datetime.fromisoformat(data["open_account_date"])
        )

    def fetchTrades(self, accountCode: str, startDt: datetime, endDt: datetime) -> list[Op]:
        url = f"accounts/{accountCode}/trades"
        params = {
            "interval.start_time": startDt.isoformat(),
            "interval.end_time": endDt.isoformat()
        }
        data = self.finamApi.get(url, params)

        return [
            Op( code=t["trade_id"],
                transDt=datetime.fromisoformat(t["timestamp"]),
                opType=self.TRADE_OP_TYPES[t["side"]],
                symbol=t["symbol"],
                quantity=Decimal(t["size"]["value"]),
                amount=(Decimal(t["price"]["value"]) * Decimal(t["size"]["value"])),
                cur=None,
                comment=t["comment"])
            for t in data["trades"]
        ]

    def fetchTrans(self, accountCode: str, startDt: datetime, endDt: datetime) -> list[Op]:
        url = f"accounts/{accountCode}/transactions"
        params = {
            "interval.start_time": startDt.isoformat(),
            "interval.end_time": endDt.isoformat()
        }
        data = self.finamApi.get(url, params)

        return [
            Op( code=t["id"],
                transDt=datetime.fromisoformat(t["timestamp"]),
                opType=t["category"],
                symbol=t["symbol"],
                quantity=noneable.apply(t.get("change_qty", {}).get("value"), Decimal),
                amount=Decimal(t["change"]["units"]) + Decimal(t["change"]["nanos"]) / 1000000000,
                cur=t["change"]["currency_code"],
                comment=t["transaction_name"])
            for t in data["transactions"]
        ]

    def validateQuantity(self, ops: list[Op]):
        invalid = [op for op in ops if op.quantity is not None and op.quantity % 1 != 0]
        if invalid:
            raise Exception(f"Fractional quantity isn't supported: {invalid}")

    def fixOpCodes(self, accountCode: str, ops: list[Op]):
        for i, op in enumerate(ops):
            code = op.code if op.code else f"{int(op.transDt.timestamp())}-{op.symbol}"
            code = f"{accountCode}-{code}"

            ops[i] = Op(
                code=code,
                transDt=op.transDt,
                opType=op.opType,
                symbol=op.symbol,
                quantity=op.quantity,
                amount=op.amount,
                cur=op.cur,
                comment=op.cur)

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

    def fetchAsset(self, accountCode: str, symbol: str) -> Asset:
        url = f"assets/{symbol}"
        params = {"account_id": accountCode}

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

    def dbLoadOps(self, curs, accountId: int, ops: list[Op]) -> None:
        log.info(f"Loading operations into DB")

        impTable = f"imp"
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
        dbLoadData(curs, impTable, ops, cols)

        stagingTable = f"staging"
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
                "sum(t.quantity) AS quantity, "
                "sum(t.amount) AS amount, "
                "t.cur, "
                "t.comment "
            f"FROM {impTable} AS t "
            "LEFT JOIN assets AS a "
                "ON a.market = SPLIT_PART(t.symbol, '@', 2) "
                    "AND a.ticker = SPLIT_PART(t.symbol, '@', 1) "
            "GROUP BY "
                "t.code, t.trans_dt, t.op_type, a.id, t.cur, t.comment;"
        )

        dbMerge(curs, "ops", stagingTable, on=("broker", "code"), cols=cols)

    def dbLinkOps(self, curs) -> None:
        log.info(f"Linking operations in DB")
        curs.execute("CALL link_ops_finam();")

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
