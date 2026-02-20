import logging as log
import requests
import json
from typing import NamedTuple
from typing import Any
from datetime import date, datetime
from decimal import Decimal

from common.config import config, initConfig
from common.logtools import initLogging
from common.dtotools import ofmethod
from common.tools import forEachSafely

import db.dbfin as dbfin
from db.dbtools import DbParams, dbConnect

@ofmethod
class Product(NamedTuple):
    productId: int
    type: str
    metalType: str
    weight: float
    name: str
    description: str

class Price(NamedTuple):
    date: date
    buyPrice: Decimal
    offlineBuyPrice: Decimal

class Ingestor:
    PROFILE = "goznak"
    MARKET = "GOZNAK"

    API_BASE_URL = "https://goznakinvest.ru/api"
    PRODUCT_LIST_URL = "v2/product/active-product"
    PRODUCT_PRICES_URL = "product/price-chart"

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
        products = self.fetchProducts()
        return forEachSafely(products, lambda product: self.processProduct(product))

    def processProduct(self, product: Product) -> None:
        log.info(f"Processing: {product.description}")

        prices = self.fetchPrices(product)
        self.dbLoad(product, prices)

    def fetchProducts(self) -> list[Product]:
        data = self.callApi(self.PRODUCT_LIST_URL, None, "GET")
        return [Product.of(a) for a in data]

    def fetchPrices(self, product: Product) -> list[Price]:
        params = {"productId": product.productId}
        data = self.callApi(self.PRODUCT_PRICES_URL, params, "POST")
        self.validateResponse(data)

        data = data["data"]["month"]
        #data = data["data"]["sixMonths"]
        #data = data["data"]["year"]

        return [
            Price(
                date=datetime.fromisoformat(p["date"]),
                buyPrice=Decimal(p["buyPrice"]),
                offlineBuyPrice=Decimal(p["offlineBuyPrice"]))
            for p in data
        ]

    def callApi(self, url: str, params: Any, method: str) -> Any:
        url = f"{self.API_BASE_URL}/{url}"
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0"}

        if method == "GET":
            response = requests.get(url, params=params, headers=headers)
        elif method == "POST":
            if params:
                headers["Content-Type"] = "application/json"
            body = json.dumps(params, separators=(",", ":"))
            response = requests.post(url, data=body, headers=headers)
        else:
            raise Exception(f"Unknown method: {method}")

        response.raise_for_status()
        return json.loads(response.text)

    def validateResponse(self, data: dict[str, Any]) -> None:
        status = data.get("status")
        if not status:
            raise ValueError(f"Invalid status, return code: {data.get("status")}")

    def dbLoad(self, product: Product, prices: list[Price]) -> None:
        prefix = f"{product.metalType}-{product.type}-{product.productId}"

        # Seems that Goznak data semantics is pretty reversed - fields with "buy" in their names are for sale prices, in fact

        data = [(p.date, p.buyPrice) for p in prices]
        self.dbLoadAsset(prefix, "online", product, data)

        data = [(p.date, p.offlineBuyPrice) for p in prices]
        self.dbLoadAsset(prefix, "offline", product, data)

    def dbLoadAsset(self, prefix: str, priceType: str, product: Product, prices: list[Price]) -> None:
        assetCode = f"{prefix}-{priceType}-sell"
        log.info(f"Loading into DB: {self.MARKET} {assetCode}")

        with self.conn.cursor() as curs:
            assetName = f"{product.description} ({priceType})"
            assetId = dbfin.dbInsertAsset(curs, self.MARKET, assetCode, assetName, str(product.weight), update=True)
            dbfin.dbInsertTrades(curs, assetId, prices, dbfin.Trades.C, dbfin.AggType.DAILY)

def main() -> int:
    ingestor = Ingestor()
    return 0 if ingestor.run() else 1

if __name__ == "__main__":
    exit(main())
