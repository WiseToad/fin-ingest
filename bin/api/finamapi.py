import logging as log
import json
import httpx
from typing import Any, Callable

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

        if response.status_code == 401:
            log.debug(f"Unauthorized, trying to recover")
            self.__updateJwtToken()
            response = perform()

        elif response.status_code == 500:
            try:
                data = json.loads(response.text)
                code = data["code"]
            except (json.JSONDecodeError, KeyError) as e:
                log.warning(f"Failed to recover after HTTP 500: {e}")
                response.raise_for_status()

            if code == 13:
                log.debug(f"Token error: {data.get("message")}")
                self.__updateJwtToken()
                response = perform()

        response.raise_for_status()
        return json.loads(response.text)

    def __updateJwtToken(self) -> None:
        log.debug("Updating token")

        url = f"{self.BASE_URL}/sessions"
        response = self.__http.post(url, json={"secret": self.__token})
        response.raise_for_status()
        
        data = json.loads(response.text)
        self.__jwtToken = data["token"]
