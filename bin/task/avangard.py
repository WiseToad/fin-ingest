import logging as log
import requests
import ssl
import locale
import re
from typing import Any
from datetime import datetime
from decimal import Decimal
from html.parser import HTMLParser
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from common.config import config, initConfig
from common.logtools import initLogging
from common.tools import forEachSafely

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

class LegacyRenegotiationAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self._pool_connections = connections
        self._pool_maxsize = maxsize
        self._pool_block = block

        ctx = ssl.create_default_context()
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT

        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx)

class Parser(HTMLParser):
    def __init__(self, tableId: str):
        super().__init__()

        self.tableId = tableId

        self.inTable = False
        self.inRow = False
        self.inCell = False

        self.cellData = []
        self.rowData = []

        self.header = None
        self.rows = []
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table" and attrs_dict.get("id") == self.tableId:
            self.inTable = True
        elif self.inTable and tag == "tr":
            self.inRow = True
            self.rowData = []
        elif self.inRow and tag in ("th", "td"):
            self.inCell = True
            self.cellData = []
            
    def handle_endtag(self, tag):
        if tag == "table" and self.inTable:
            self.inTable = False
        elif tag == "tr" and self.inRow:
            self.inRow = False
            if self.rowData:
                self.rows.append(self.rowData)
        elif tag == "th" and self.inCell:
            self.inCell = False
            if self.header is None:
                self.header = "".join(self.cellData).strip()
        elif tag == "td" and self.inCell:
            self.inCell = False
            self.rowData.append("".join(self.cellData).strip())
            
    def handle_data(self, data):
        if self.inCell:
            self.cellData.append(data)

class Ingestor:
    PROFILE = "avangard"
    MARKET = "AVANGARD"

    PAGE_URL = "https://www.avangard.ru/rus/private/preciousmetal/goldbrick"

    conn: Any

    def __init__(self):
        initConfig(self.PROFILE)
        initLogging(config.get("logLevel"))

    def run(self) -> bool:
        self.conn = dbConnect(DbParams.of(config["db"]))
        try:
            return self.process()
        finally:
            self.conn.close()

    def process(self) -> bool:
        log.info(f"Fetching HTML")
        html = self.fetchHtml()

        tables = ("rate_list", "rate_list_new")
        return forEachSafely(tables, lambda table: self.parseTable(html, table))

    def fetchHtml(self) -> str:
        session = requests.Session()
        session.mount("https://", LegacyRenegotiationAdapter())

        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0"}
        response = session.get(self.PAGE_URL, headers=headers)
        response.raise_for_status()

        return response.text

    def parseTable(self, html: str, tableId: str) -> bool:
        log.info(f"Processing: {tableId}")

        parser = Parser(tableId)
        parser.feed(html)

        if parser.header is None or not parser.rows:
            log.warning(f"No data found for table {tableId} in HTML")
            return True

        match = re.search(r"Котировки по состоянию на ([0-9]+ [^ ]+ [0-9]+) года", parser.header)
        if not match:
            log.error(f"Failed to parse header of table {tableId}")
            return False

        locale.setlocale(locale.LC_TIME, "ru_RU.UTF-8")
        dt = datetime.strptime(match.group(1), "%d %B %Y")

        log.info(f"Parsed {len(parser.rows)} rows for {dt.date().isoformat()}")

        return forEachSafely(parser.rows, lambda r: self.dbLoad(r[0], dt, Decimal(r[2].replace(" ", ""))))

    def dbLoad(self, unit: str, dt: datetime, price: Decimal) -> None:
        assetCode = f"gold-{unit}-sell"
        log.info(f"Loading into DB: {self.MARKET} {assetCode}")

        with self.conn.cursor() as curs:
            assetName = f"{self.MARKET} {assetCode}"
            assetId = dbfin.dbInsertAsset(curs, self.MARKET, assetCode, assetName, unit)
            dbfin.dbInsertTrades(curs, assetId, [(dt, price)], dbfin.Trades.C, dbfin.AggType.DAILY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
