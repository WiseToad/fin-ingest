import logging as log
from typing import Iterable

from db.dbtools import DbTypes, ColumnDef, MergeMode
from db.dbtools import dbTempTable, dbLoadData, dbMerge, dbMergeRow

class Assets:
    MARKET = ColumnDef("MARKET", DbTypes.VARCHAR(15))
    CODE = ColumnDef("CODE", DbTypes.VARCHAR(25))
    NAME = ColumnDef("NAME", DbTypes.VARCHAR)
    UNIT = ColumnDef("UNIT", DbTypes.VARCHAR(15))

class Trades:
    DT = ColumnDef("dt", DbTypes.TIMESTAMPTZ)
    O = ColumnDef("o", DbTypes.DECIMAL(20, 4))
    H = ColumnDef("h", DbTypes.DECIMAL(20, 4))
    L = ColumnDef("l", DbTypes.DECIMAL(20, 4))
    C = ColumnDef("c", DbTypes.DECIMAL(20, 4))
    V = ColumnDef("v", DbTypes.BIGINT)
    UNIT = ColumnDef("UNIT", DbTypes.VARCHAR(15))

class AggType:
    INTRADAY = "I"
    SNAPSHOT = "S"
    DAILY = "D"
    OPEN = "O" # for balance, etc
    CLOSE = "C" # for balance, etc

def dbInsertAsset(curs, 
                  market: str,
                  code: str,
                  name: str = None,
                  unit: str = None, *,
                  update: bool = False) -> int:

    if name is None:
        name = f"{market} {code}"
        if unit is not None:
            name = f"{name}, {unit}"

    log.debug(f"Updating asset: {name}")

    mergeMode = MergeMode.MERGE if update else MergeMode.INSERT

    with curs.connection:
        row = {"market": market, "code": code, "name": name, "unit": unit}
        return dbMergeRow(curs, "assets", row, key=("market", "code"), returning="id", mode=mergeMode)

def dbInsertTrades(curs,
                   assetId: int,
                   data: Iterable,
                   valueCols: Iterable[ColumnDef],
                   aggType: str, *,
                   update: bool = True) -> None:

    log.debug(f"Loading trades for asset id: {assetId}")

    if valueCols is None:
        valueCols = (Trades.C,)
    elif isinstance(valueCols, (ColumnDef, str)) or not isinstance(valueCols, Iterable):
        valueCols = (valueCols,)

    mergeMode = MergeMode.MERGE if update else MergeMode.INSERT

    with curs.connection:
        cols = (Trades.DT,) + valueCols
        dbTempTable(curs, "temp", cols)
        dbLoadData(curs, "temp", data, cols)

        colNames = [c.name for c in valueCols]
        dbMerge(curs, "trades", "temp", on={"asset_id": assetId, "agg_type": aggType, "dt": ColumnDef("dt")}, data=colNames, mode=mergeMode)
