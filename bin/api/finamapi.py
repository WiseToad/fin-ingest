import logging as log
import json
import httpx
from typing import Any, Callable

class FinamApi:
    API_BASE_URL = "https://api.finam.ru/v1"

    http: httpx.Client

    token: str
    jwtToken: str

    def __init__(self, http: httpx.Client, token: str):
        self.http = http
        self.token = token
        self.jwtToken = None

    def get(self, url: str, params: dict[str, Any]) -> Any:
        return self.__call(lambda: self.__get(url, params))

    def __get(self, url: str, params: dict[str, Any]) -> httpx.Response:
        url = f"{self.API_BASE_URL}/{url}"
        return self.http.get(url, params=params, headers={"Authorization": self.jwtToken})

    def getAccountIds(self) -> list[str]:
        data = self.__call(self.__getAccountIds)
        return data["account_ids"]

    def __getAccountIds(self) -> httpx.Response:
        url = f"{self.API_BASE_URL}/sessions/details"
        return self.http.post(url, json={"token": self.jwtToken})

    def __call(self, perform: Callable[[], httpx.Response]) -> Any:
        if not self.jwtToken:
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

        url = f"{self.API_BASE_URL}/sessions"
        response = self.http.post(url, json={"secret": self.token})
        response.raise_for_status()
        
        data = json.loads(response.text)
        self.jwtToken = data["token"]
