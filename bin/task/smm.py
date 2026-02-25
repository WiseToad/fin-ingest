import logging as log
import requests
import json
from typing import NamedTuple
from typing import Any, Callable
from datetime import date
from decimal import Decimal
from itertools import accumulate

from common.config import config, initConfig
from common.logtools import initLogging
from common.dtotools import ofmethod
from common.tools import getPeriodFromArgv, forEachSafely, toIterable
from common.datetools import minusMonth

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

@ofmethod
class Category(NamedTuple):
    secondName: str
    productCodes: list[str]
    priceType: str = None

@ofmethod
class Product(NamedTuple):
    product_id: str
    product_code: str
    product_name: str
    unit: str

class Price(NamedTuple):
    dt: date
    value: Decimal
    unit: str = None

class PriceChange(NamedTuple):
    dt: date
    value: Decimal
    change: Decimal

class Ingestor:
    PROFILE = "smm"
    MARKET = "SMM"

    API_BASE_URL = "https://platform.metal.com/spotoverseascenter/v1"
    PRODUCT_LIST_URL = "prices/product_list"
    PRICE_HISTORY_URL = "product_info/history/{}"

    PRICE_PARSERS: dict[str, Callable]

    conn: Any

    def __init__(self):
        initConfig(self.PROFILE)
        initLogging(config.get("logLevel"))

        self.PRICE_PARSERS = {
            "PRICE": self.parsePrices,
            "RATE": self.parseRates
        }

    def run(self) -> bool:
        today = date.today()
        startDate, endDate = getPeriodFromArgv(minusMonth(today), today)

        self.conn = dbConnect(DbParams.of(config["db"]))
        try:
            return self.process(startDate, endDate)
        finally:
            self.conn.close()

    def process(self, startDate: date, endDate: date) -> bool:
        categories = config.get("categories", [])
        return forEachSafely(categories, lambda c: self.processCategory(Category.of(c), startDate, endDate))

    def processCategory(self, category: Category, startDate: date, endDate: date) -> bool:
        log.info(f"Processing: {category.secondName}, period: {startDate.isoformat()} to {endDate.isoformat()}")

        products = self.fetchProducts(category)

        parser = self.PRICE_PARSERS[category.priceType or "PRICE"]

        return forEachSafely(products, lambda product: self.processProduct(product, startDate, endDate, parser))

    def fetchProducts(self, category: Category) -> list[Product]:
        productCodes = toIterable(category.productCodes)

        params = {"second_name": category.secondName, "currency_type": 1}
        data = self.callApi(self.PRODUCT_LIST_URL, params)
        self.validateResponse(data)

        products = [
            Product.of(p)
            for c in data["data"]["category_list"]
            for p in c["products"]
            if p["product_code"] in productCodes
        ]

        productCount = len(products)
        expectedCount = len(productCodes)
        if productCount != expectedCount:
            log.warning(f"Requested {expectedCount} products, but fetched {productCount}")

        return products

    def processProduct(self, product: Product, startDate: date, endDate: date, parser: Callable) -> None:
        log.info(f"Processing product: {product.product_code} {product.unit}")

        prices = self.fetchPrices(product, startDate, endDate, parser)
        if not prices:
            log.warning(f"No prices retrieved")
            return

        key = lambda p: p.dt
        count, start, end = len(prices), min(prices, key=key), max(prices, key=key)
        log.info(f"Fetched {count} prices, period: {start.dt.isoformat()} to {end.dt.isoformat()}")

        self.dbLoad(product, prices)

    def fetchPrices(self, product: Product, startDate: date, endDate: date, parser: Callable) -> list[Price]:
        url = self.PRICE_HISTORY_URL.format(product.product_id)
        params = {
            "begin_date": startDate.isoformat(),
            "end_date": endDate.isoformat(),
            "currency_type": 1
        }
        data = self.callApi(url, params)
        self.validateResponse(data)

        prices = data["data"]["prices"]
        return parser(prices) if prices else []
            
    def callApi(self, url: str, params: dict[str, Any]) -> Any:
        url = f"{self.API_BASE_URL}/{url}"
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
        response = requests.get(url, params, headers=headers)
        response.raise_for_status()
        return json.loads(response.text)

    def validateResponse(self, data: dict[str, Any]) -> None:
        code = data.get("code")
        if code != 0:
            raise ValueError(f"Invalid return code: {code}, details: {data.get("msg")}")

    def parsePrices(self, prices: list[dict[str, Any]]) -> list[Price]:
        units = set(p["unit"] for p in prices)
        if len(units) > 1:
            raise ValueError(f"Different price units encountered in period: {units}")
        unit = units.pop()

        changes = (
            PriceChange(
                dt=date.fromisoformat(p["renew_date"]),
                value=Decimal(p["average"]),
                change=Decimal(p["change"]))
            for p in prices
        )
        changes = sorted(changes, key=lambda c: c.dt, reverse = True)
        changes = accumulate(changes, func=lambda p, n: PriceChange(n.dt, p.value - p.change, n.change))

        return [Price(c.dt, c.value, unit) for c in changes]

    def parseRates(self, prices: list[dict[str, Any]]) -> list[Price]:
        return [
            Price(
                dt=date.fromisoformat(p["renew_date"]),
                value=Decimal(p["rate"]))
            for p in prices
        ]

    def dbLoad(self, product: Product, prices: list[Price]) -> None:
        log.info(f"Loading into DB: {self.MARKET} {product.product_code}")

        with self.conn.cursor() as curs:
            assetId = dbfin.dbInsertAsset(curs, self.MARKET, product.product_code, product.product_name, product.unit, update=True)
            valueCols = (dbfin.Trades.C, dbfin.Trades.UNIT)
            dbfin.dbInsertTrades(curs, assetId, prices, valueCols, dbfin.AggType.DAILY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
