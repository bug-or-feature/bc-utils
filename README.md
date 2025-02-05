# bc-utils

[Barchart.com](https://www.barchart.com) allows registered users to download historic futures contract prices in CSV format. Individual contracts must be downloaded separately, which is laborious and slow. This script automates the process.

## Quickstart

```
import os
import logging
from bcutils.bc_utils import get_barchart_downloads, create_bc_session

logging.basicConfig(level=logging.INFO)

CONTRACTS = {
    "AUD" : {"code": "A6", "cycle": "HMUZ", "exchange": "CME"},
    "GOLD" : {"code": "GC", "cycle": "GJMQVZ", "exchange": "COMEX"},
}

session = create_bc_session(
    config_obj=dict(
        barchart_username="user@domain.com",
        barchart_password="s3cr3t_321",
    )
)

get_barchart_downloads(
    session,
    contract_map=CONTRACTS,
    instr_list=["AUD", "GOLD"],
    save_dir=os.getcwd(),
    start_year=2020,
    end_year=2021,
)
```

The code above would: 
* for the CME Australian Dollar future, get OHLCV price data for the Mar, Jun, Sep and Dec 2020 contracts
* download in CSV format
* save with filenames like Hour_AUD_20200300.csv, Day_AUD_20200600.csv into the specified directory
* for COMEX Gold, get Feb, Apr, Jun, Aug, Oct, and Dec data, with filenames like Hour_GOLD_20200200.csv etc

Features:
* Designed to be run once a day by a scheduler
* the script skips contracts already downloaded
* by default gets 120 days of data per contract, override possible per instrument
* dry run mode to check setup
* allows updates to previously downloaded files
* you must be a registered user. Paid subscribers get 250 downloads a day, otherwise 5

## For pysystemtrade users

This project was originally created to make it easier to populate [pysystemtrade](https://github.com/robcarver17/pysystemtrade) (PST) with futures prices from Barchart, so setup is straightforward. Steps:

1. Clone the bc-utils repo, or your own fork. The remaining steps assume the location `~/bc-utils`

2. Have a look at `~/bc-utils/bcutils/config.py`. This file already contains config items for many futures instruments, with their matching PST symbols. For example, 
```python
CONTRACT_MAP = {
    ...
    "GOLD": {"code": "GC", "cycle": "GJMQVZ", "exchange": "COMEX"},
    ...
}
```
indicates that instrument with PST code **GOLD** has the Barchart symbol **GC**, months **GJMQVZ**, and exchange **COMEX**. It also contains date config for various Futures exchanges. For example,

```python
EXCHANGES = {
    ...
    "COMEX": {"tick_date": "2008-05-04", "eod_date": "1978-02-27"},
    ...
}
```

That indicates that futures instruments with exchange **COMEX** have daily price data from 27 Feb 1978, and hourly from 4 May 2008. Those date attributes are [provided by Barchart](https://www.barchart.com/solutions/data/market), and turn out to be inaccurate. Previous versions of this library would waste valuable allowance by attempting to download data that was not there. Newer versions handle this much better. If you use this library to download prices that are not in the config file, please consider contributing with a PR.

3. Have a look at the sample snippets in `~bc-utils/sample/pst.py`. There are examples for use with an external config file. Use the sample config `~bc-utils/sample/private_config_sample.yaml`, copy and rename to the top level of the `~bc-utils` dir. Update with your credentials and save path etc

Alternatively, paste the contents of the sample config into your PST private config, and do something like

```python
def download_with_pst_config():
    config = load_config("<path to your PST private config>")
    get_barchart_downloads(
        create_bc_session(config),
        instr_list=config['barchart_download_list'],
        start_year=config['barchart_start_year'],
        end_year=config['barchart_end_year'],
        save_dir=config['barchart_path'],
        do_daily=config['barchart_do_daily'],
        dry_run=config['barchart_dry_run'],
    )
```

4. add bc-utils to your crontab
```
00 08 * * 1-7 . $HOME/.profile; cd ~/bc-utils; python3 bcutils/sample/pst.py >> $ECHO_PATH/barchart_download.txt 2>&1
```
You could also add another entry to run the updater once a week.

5. To import the prices into PST (_see below_):

```python
from sysdata.config.production_config import get_production_config
from syscore.fileutils import resolve_path_and_filename_for_package
from sysdata.csv.csv_futures_contract_prices import ConfigCsvFuturesPrices
from sysinit.futures.contract_prices_from_split_freq_csv_to_db import (
    init_db_with_split_freq_csv_prices_for_code,
)

BARCHART_CONFIG = ConfigCsvFuturesPrices(
    input_date_index_name="Time",
    input_skiprows=0,
    input_skipfooter=0,
    input_date_format="%Y-%m-%dT%H:%M:%S%z",
    input_column_mapping=dict(
        OPEN="Open", HIGH="High", LOW="Low", FINAL="Close", VOLUME="Volume"
    ),
)

# assuming bc-utils config pasted into private
datapath = resolve_path_and_filename_for_package(
    get_production_config().get_element_or_default("barchart_path", None)
)

# import prices for a single instrument
init_db_with_split_freq_csv_prices_for_code("GOLD", datapath=datapath, csv_config=BARCHART_CONFIG)
```
