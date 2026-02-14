A set of simple data ingestors for financial analysis.

## DISCLAIMER

This project was created for private usage in mind. The main goal to push it to this repo was not for distribution, but for preserve it's source code and to leverage version control. So, no effort made to maintain it's docs, to reach smoothness of version upgrades, to adhere to any distribution license, and so on.

However, anyone can study or use this project in any way at their own risk.

## PREREQUISITES

To use Selenium with Mozilla on headless servers (required for some ingestor tasks):

```sh
sudo apt install -y xvfb libgtk-3-0 libasound2
```

## INSTALL

### Deploy package

```sh
sudo mkdir -p /opt/fin-ingest
cd /opt/fin-ingest

wget -q -O - https://github.com/WiseToad/fin-ingest/releases/latest/download/fin-ingest.tar.gz | sudo tar -xzf -

sudo useradd -r -d /opt/fin-ingest -s /usr/sbin/nologin fin-ingest
sudo chgrp fin-ingest bin/fin-ingest

sudo cp -p config/fin-ingest.toml.sample config/fin-ingest.toml
sudo chgrp fin-ingest config/fin-ingest.toml
```

### Prepare Python environment

```sh
cd /opt/fin-ingest
sudo python3 -m venv venv
sudo bash -c "source venv/bin/activate && pip install -r requirements.txt"
```

### Prepare Selenium cache

```sh
sudo mkdir -p cache
sudo chmod 2775 cache
sudo chgrp fin-ingest cache
```

### Setup the DB

Apply DB scripts from `db` directory.

## CONFIGURE

Just basic guidelines given. For details, see source code.

### Basic configuration

- Configure DB connection in `config/fin-ingest.toml` file.
- Add config files into `config/task` directory for specific tasks, located in `bin/task`.
- Configure systemd units and timers to setup periodic task schedule.

### Environment variables

Variables below are optional, but may be reassigned for some specific cases - for developer environment, for non-typical deployments, and so on.

- `FIN_INGEST_VENV_DIR` - Python virtual environment location.
- `FIN_INGEST_CONFIG_DIR` - location of `fin-ingest.toml`.
- `FIN_INGEST_CONFIG_FILE` - full path of main config file (the alternative of above)
- `FIN_INGEST_CACHE_DIR` - mainly for Selenium and browser stuff.

Third-party:

- `SE_CACHE_PATH` - location of Selenium cache with web drivers and browser runtime.

## TROUBLESHOOTING

- Set `logLevel="DEBUG"` parameter in config file, either common or specific for a task.
- If problem is Selenium related, then launch browser by hand with `-headless` option from Selenium cache directory and resolve errors printed to the console.
