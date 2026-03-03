import logging as log
import json
import httpx
from typing import Any, Callable

class FinamError(Exception):
    http: int
    code: int
    message: str

    def __init__(self, http: str, code: int, message: str = None):
        self.http = http
        self.code = code
        self.message = message

    def __str__(self):
        return f"HTTP {self.http}, code: {self.code}, message: {self.message}"

class FinamApi:
    BASE_URL = "https://api.finam.ru/v1"

    __http: httpx.Client

    __token: str
    __jwtToken: str

    def __init__(self, http: httpx.Client, token: str):
        self.__http = http
        self.__token = token
        self.__jwtToken = None

    def get(self, url: str, params: dict[str, Any]) -> Any:
        return self.__call(lambda: self.__get(url, params))

    def __get(self, url: str, params: dict[str, Any]) -> httpx.Response:
        url = f"{self.BASE_URL}/{url}"
        return self.__http.get(url, params=params, headers={"Authorization": self.__jwtToken})

    def getAccountIds(self) -> list[str]:
        data = self.__call(self.__getAccountIds)
        return data["account_ids"]

    def __getAccountIds(self) -> httpx.Response:
        url = f"{self.BASE_URL}/sessions/details"
        return self.__http.post(url, json={"token": self.__jwtToken})

    def __call(self, perform: Callable[[], httpx.Response]) -> Any:
        if not self.__jwtToken:
            self.__updateJwtToken()

        response = perform()

        match response.status_code:
            case 401:
                log.debug(f"Unauthorized, trying to recover")
                self.__updateJwtToken()
                response = perform()

            case 500:
                err = self.__getError(response)
                if err is None:
                    response.raise_for_status()

                if err.code == 13:
                    log.debug(f"Token error to be recovered: {err.message}")
                    self.__updateJwtToken()
                    response = perform()
                else:
                    raise err

            case 400:
                err = self.__getError(response)
                if err is None:
                    response.raise_for_status()

                raise err

        response.raise_for_status()
        return json.loads(response.text)

    def __getError(self, response: httpx.Response) -> FinamError:
        try:
            err = json.loads(response.text)
            return FinamError(http=response.status_code, code=err["code"], message=err.get("message"))
        except (json.JSONDecodeError, TypeError):
            return None

    def __updateJwtToken(self) -> None:
        log.debug("Updating token")

        url = f"{self.BASE_URL}/sessions"
        response = self.__http.post(url, json={"secret": self.__token})
        response.raise_for_status()
        
        data = json.loads(response.text)
        self.__jwtToken = data["token"]
