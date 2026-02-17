import os, getpass
import time
import json
import logging as log
from typing import Any
from urllib.parse import urlencode

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By

__CACHE_DIR_ENV = "FIN_INGEST_CACHE_DIR"
__SE_CACHE_DIR_ENV = "SE_CACHE_PATH"

__CACHE_DEFAULT_DIR = "/opt/fin-ingest/cache"

__FETCH_TIMEOUT = 10

def initWebDriver() -> Any:
    __initEnv()

    options = Options()
    options.add_argument("-headless")
    options.add_argument("-private")
    options.set_preference("devtools.jsonview.enabled", False)

    return webdriver.Firefox(options=options)

def __initEnv():
    cacheDir = os.environ.get(__CACHE_DIR_ENV)
    if cacheDir is None and os.path.isdir(__CACHE_DEFAULT_DIR):
        cacheDir = __CACHE_DEFAULT_DIR
    if cacheDir is None:
        return

    os.environ.setdefault(__SE_CACHE_DIR_ENV, f"{cacheDir}/selenium")

    # Used by Firefox
    xdgBaseDir = f"{cacheDir}/xdg/{getpass.getuser()}"
    xdgConfigDir = f"{xdgBaseDir}/.config"
    xdgCacheDir = f"{xdgBaseDir}/.cache"
    xdgDownloadDir = f"{xdgBaseDir}/Downloads"

    os.environ["XDG_CONFIG_HOME"] = xdgConfigDir
    os.environ["XDG_CACHE_HOME"] = xdgCacheDir

    os.makedirs(xdgConfigDir, exist_ok=True)
    with open(f"{xdgConfigDir}/user-dirs.dirs", "w") as f:
        f.write(f"XDG_DOWNLOAD_DIR=\"{xdgDownloadDir}\"\n")

def callApiNoF5(driver: Any, url: str, params: dict[str, Any] = None) -> Any:
    if params:
        url = f"{url}?{urlencode(params)}"

    log.debug(f"Fetching: GET {url}")

    driver.get(url)

    maxTime = time.time() + __FETCH_TIMEOUT
    while time.time() < maxTime:
        pageSource = driver.page_source

        if "TSPD" in pageSource or "bobcmn" in pageSource:
            time.sleep(1)
            continue

        if "application/json" in driver.execute_script("return document.contentType || ''"):
            log.debug("Got an application/json content!")
            data = driver.find_element(By.TAG_NAME, "body").text
            break

        time.sleep(0.5)
    else:
        raise Exception(f"Failed to fetch data in {__FETCH_TIMEOUT} sec")

    return json.loads(data)
