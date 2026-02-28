import logging as log

from db.dbtools import DbTypes, ColumnDef, MergeMode
from db.dbtools import dbTempTable, dbLoadData, dbMerge, dbMergeRow

class Accounts:
    BROKER = ColumnDef("BROKER", DbTypes.VARCHAR(15))
    CODE = ColumnDef("CODE", DbTypes.VARCHAR(15))
    NAME = ColumnDef("NAME", DbTypes.VARCHAR)

def dbInsertAccount(curs, 
                    broker: str,
                    code: str,
                    name: str = None, *,
                    update: bool = False) -> int:

    if name is None:
        name = f"{broker} {code}"

    log.debug(f"Updating account: {name}")

    mergeMode = MergeMode.MERGE if update else MergeMode.INSERT

    with curs.connection:
        row = {"broker": broker, "code": code, "name": name}
        return dbMergeRow(curs, "accounts", row, key=("broker", "code"), returning="id", mode=mergeMode)
