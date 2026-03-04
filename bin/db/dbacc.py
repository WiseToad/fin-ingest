import logging as log

from db.dbtools import DbTypes, ColumnDef, MergeMode
from db.dbtools import dbMergeRow

class Accounts:
    BROKER = ColumnDef("broker", DbTypes.VARCHAR(15))
    CODE = ColumnDef("code", DbTypes.VARCHAR(15))
    NAME = ColumnDef("name", DbTypes.VARCHAR)
    COMMENT = ColumnDef("comment", DbTypes.VARCHAR)

class Assets:
    MARKET = ColumnDef("market", DbTypes.VARCHAR(15))
    TICKER = ColumnDef("ticker", DbTypes.VARCHAR(15))
    ISIN = ColumnDef("isin", DbTypes.VARCHAR(12))
    NAME = ColumnDef("name", DbTypes.VARCHAR)
    CUR = ColumnDef("cur", DbTypes.VARCHAR(3))

class Ops:
    BROKER = ColumnDef("broker", DbTypes.VARCHAR(15))
    CODE = ColumnDef("code", DbTypes.VARCHAR(50))
    CORR_ID = ColumnDef("corr_id", DbTypes.BIGINT)
    ACCOUNT_ID = ColumnDef("account_id", DbTypes.BIGINT)
    TRANS_DT = ColumnDef("trans_dt", DbTypes.TIMESTAMPTZ)
    SETTLE_DT = ColumnDef("settle_dt", DbTypes.TIMESTAMPTZ)
    OP_TYPE = ColumnDef("op_type", DbTypes.VARCHAR(15))
    ASSET_ID = ColumnDef("asset_id", DbTypes.BIGINT)
    QUANTITY = ColumnDef("quantity", DbTypes.BIGINT)
    AMOUNT = ColumnDef("amount", DbTypes.DECIMAL(20, 4))
    CUR = ColumnDef("cur", DbTypes.VARCHAR(3))
    COMMENT = ColumnDef("comment", DbTypes.VARCHAR)

def dbInsertAccount(curs, 
                    broker: str,
                    code: str,
                    name: str = None, *,
                    update: bool = False) -> int:

    if name is None:
        name = f"{broker} {code}"

    log.debug(f"Updating account: {name}")

    mergeMode = MergeMode.MERGE if update else MergeMode.INSERT

    row = {"broker": broker, "code": code, "name": name}
    return dbMergeRow(curs, "accounts", row, key=("broker", "code"), returning="id", mode=mergeMode)
