import logging as log
from typing import Any, Iterable, Callable
from typing import NamedTuple
from enum import Enum

import psycopg2, psycopg2.extras

from common.dtotools import ofmethod

@ofmethod
class DbParams(NamedTuple):
    host: str = "localhost"
    port: int = 5432
    dbname: str = "postgres"
    user: str = None
    password: str = None

class DbTypes:
    VARCHAR = lambda n = None: "VARCHAR" if n is None else f"VARCHAR({n})"
    BIGINT = "BIGINT"
    DECIMAL = lambda s, p: f"DECIMAL({s}, {p})"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"

class ColumnDef(NamedTuple):
    name: str
    type: str = DbTypes.VARCHAR

class SqlParam(NamedTuple):
    placeholder: str = "%s"

class SqlExpr(NamedTuple):
    expr: str

class MergeMode(Enum):
    MERGE = 1
    INSERT = 2
    UPDATE = 3

__SQL_CONVERTERS: dict[type, Callable] = {
    type(None): lambda v: "NULL",
    str: lambda v: f"'{v}'",
    SqlParam: lambda v: f"{v.placeholder}",
    SqlExpr: lambda v: f"{v.expr}"
}

def dbConnect(params: DbParams):
    log.info(f"Connecting to: postgresql://{params.host}:{params.port}/{params.dbname}")
    conn = psycopg2.connect(
        host=params.host,
        port=params.port,
        dbname=params.dbname,
        user=params.user,
        password=params.password)

    with conn.cursor() as curs:
        curs.execute("SELECT version()")
        log.debug(f"Connected to: {curs.fetchone()[0]}")

    return conn

def dbTempTable(curs, tableName: str, cols: Iterable[ColumnDef], onCommit: str = "DROP") -> None:
    log.debug(f"Creating temp table: {tableName}")

    sqlCols = ", ".join(f"{c.name} {c.type}" for c in cols)
    sql = f"CREATE TEMPORARY TABLE {tableName} ({sqlCols}) ON COMMIT {onCommit};"
    log.debug(f"Query: {sql}")

    curs.execute(sql)

def dbLoadData(curs, tableName: str, data: Iterable[Any], cols: Iterable[str | ColumnDef]) -> None:
    log.debug(f"Inserting data into table: {tableName}")

    sqlCols = ", ".join(c.name if isinstance(c, ColumnDef) else c for c in cols)
    sql = f"INSERT INTO {tableName} ({sqlCols}) VALUES %s"
    log.debug(f"Query: {sql}")

    psycopg2.extras.execute_values(curs, sql, data, page_size=1000)

def dbLoadCsv(curs, tableName: str, fileName: str, cols: Iterable[str | ColumnDef], sep=",") -> None:
    log.debug(f"Loading CSV: {fileName} into table: {tableName}")

    with open(fileName, 'r') as f:
        header = next(f).rstrip("\r\n")
        log.debug(f"Skipping CSV header: \"{header}\"")

        cols = [c.name if isinstance(c, ColumnDef) else c for c in cols]
        curs.copy_from(f, tableName, sep=sep, columns=cols)

    log.debug(f"CSV loaded")

def dbMerge(curs,
            targetTable: str,
            sourceTable: str, *,
            on: Iterable[str] | dict[str, Any],
            data: Iterable[str] | dict[str, Any] = None,
            params: Iterable[Any] = None,
            mode: MergeMode = None) -> None:

    log.debug(f"Merging into: {targetTable}, from: {sourceTable}")

    targetAlias, sourceAlias = "t", "s"

    on = __rowToSql(__toDict(on, ColumnDef), sourceAlias)
    data = __rowToSql(__toDict(data, ColumnDef), sourceAlias)

    insert = on | data
    update = data

    if mode == MergeMode.INSERT:
        update = ()
    elif mode == MergeMode.UPDATE:
        insert = ()
    elif mode is not None and mode != MergeMode.MERGE:
        raise Exception(f"Invalid merge mode: {mode}")

    if not insert and not update:
        log.debug("Nothing to merge")
        return

    sqlOn = " AND ".join(f"{targetAlias}.{col} = {val}" for col, val in on.items())
    sql = (
        f"MERGE INTO {targetTable} AS {targetAlias} " +
        f"USING {sourceTable} AS {sourceAlias} " +
        f"ON ({sqlOn})")

    if insert:
        sqlInsert = ", ".join(insert.keys())
        sqlValues = ", ".join(insert.values())
        sql += f" WHEN NOT MATCHED THEN INSERT ({sqlInsert}) VALUES ({sqlValues})"

    if update:
        sqlWhere = " OR ".join(f"{targetAlias}.{col} IS DISTINCT FROM {val}" for col, val in update.items())
        sqlUpdate = ", ".join(f"{col} = {val}" for col, val in update.items())
        sql += f" WHEN MATCHED AND ({sqlWhere}) THEN UPDATE SET {sqlUpdate}"

    sql += ";"
    log.debug(f"Query: {sql}")

    if params:
        log.debug(f"Params: {params}")
        params = [p for p in params] + [p for p in params]

    curs.execute(sql, params)
    log.debug(f"Merge done")

def dbMergeRow(curs,
               targetTable: str,
               row: dict[str, Any] | Iterable[str], *,
               params: Iterable[Any] = None,
               key: Iterable[str],
               returning: Iterable[str] = None,
               mode: MergeMode = None) -> Any | None:

    log.debug(f"Merging row into: {targetTable}")

    targetAlias = "t"

    row = __rowToSql(__toDict(row, lambda v: SqlParam()))
    key = __toIterable(key)
    returning = __toIterable(returning)

    sqlInsert = ", ".join(row.keys())
    sqlValues = ", ".join(row.values())

    sql = f"INSERT INTO {targetTable} AS {targetAlias} ({sqlInsert}) VALUES ({sqlValues})"

    if mode is None or mode == MergeMode.MERGE:
        keySet = {col.casefold() for col in key}
        update = [col for col in row.keys() if col.casefold() not in keySet]

        sqlOnConflict = ", ".join(key)
        sqlUpdate = ", ".join(f"{col} = EXCLUDED.{col}" for col in update)
        sqlWhere = " OR ".join(f"{targetAlias}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in update)

        sql += f" ON CONFLICT ({sqlOnConflict}) DO UPDATE SET {sqlUpdate} WHERE {sqlWhere}"
    elif mode == MergeMode.INSERT:
        sql += " ON CONFLICT DO NOTHING"
    else:
        raise Exception(f"Invalid or unsupported merge mode: {mode}")

    if returning:
        sqlReturning = ", ".join(returning)
        sqlWhere = " AND ".join(f"{targetAlias}.{col} IS NOT DISTINCT FROM {row.get(col)}" for col in key)

        sql = (
            f"WITH q AS ({sql} RETURNING {sqlReturning}) " + 
            f"SELECT * FROM q UNION ALL " + 
            f"SELECT {sqlReturning} FROM {targetTable} AS {targetAlias} " +
            f"WHERE {sqlWhere} AND NOT EXISTS (SELECT NULL FROM q)")

    sql += ";"
    log.debug(f"Query: {sql}")

    if params:
        log.debug(f"Params: {params}")
        if returning:
            params = [p for p in params] + [p for p in params]

    curs.execute(sql, params)

    retVals = None
    if returning:
        retVals = curs.fetchone()
        if len(returning) == 1:
            retVals = retVals[0]
        else:
            retVals = {col: retVals[i] for i, col in enumerate(returning)}
        log.debug(f"Return: {retVals}")

    log.debug(f"Row merged")
    return retVals

def __toIterable(value: Any) -> Iterable:
    if value is None:
        value = ()
    elif isinstance(value, (ColumnDef, str)) or not isinstance(value, Iterable):
        value = (value,)
    return value

def __toDict(value: Any, mapper: Callable) -> dict:
    if not isinstance(value, dict):
        value = {v: mapper(v) for v in __toIterable(value)}
    return value

def __rowToSql(row: dict[str, Any], tableAlias: str = None) -> dict[str, str]:
    return {col: __valueToSql(val, tableAlias) for col, val in row.items()}

def __valueToSql(value: Any, tableAlias: str = None) -> str:
    if isinstance(value, ColumnDef):
        return value.name if tableAlias is None else f"{tableAlias}.{value.name}"
    return __SQL_CONVERTERS.get(type(value), str)(value)
