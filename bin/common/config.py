import os, tomllib
from typing import Any

config: dict[str, Any] = {}

__CONFIG_FILE_ENV = "FIN_INGEST_CONFIG_FILE"
__CONFIG_DIR_ENV = "FIN_INGEST_CONFIG_DIR"

__CONFIG_DEFAULT_DIRS = [
    "/opt/fin-ingest/config",
    "/etc/fin-ingest"]

__CONFIG_FILE = "fin-ingest.toml"
__PROFILE_FILE = "task/{}.toml"

def initConfig(profile: str = None) -> None:
    configFile = __getConfigFile()

    if configFile is None:
        configDir = __getConfigDir()
        if configDir is not None:
            configFile = __findFile(configDir, __CONFIG_FILE)
    else:
        configDir = os.path.dirname(configFile)

    global config
    config.clear()

    if configFile is not None:
        config |= __loadConfig(configFile)

    if profile is not None and configDir is not None:
        profileFile = __findFile(configDir, __PROFILE_FILE.format(profile))
        if profileFile is not None:
            config |= __loadConfig(profileFile)

    config |= os.environ

def __getConfigFile() -> str | None:
    return os.environ.get(__CONFIG_FILE_ENV)

def __getConfigDir() -> str | None:
    configDir = os.environ.get(__CONFIG_DIR_ENV)
    if configDir is not None:
        return configDir

    configDirs = (d for d in __CONFIG_DEFAULT_DIRS if os.path.isdir(d))
    return next(configDirs, None)

def __findFile(dir: str, fileName: str) -> str | None:
    fileName = os.path.join(dir, fileName)
    if os.path.isfile(fileName):
        return fileName
    return None

def __loadConfig(configFile: str) -> dict[str, Any]:
    with open(configFile, "rb") as f:
        return tomllib.load(f)
