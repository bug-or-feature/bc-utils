# bc-utils

[Barchart.com](https://www.barchart.com) allows registered users to download historic futures contract prices in CSV 
format. Individual contracts must be downloaded separately, which is laborious and slow. This script automates the process.

## Quickstart

```
from bcutils.bc_utils import get_barchart_downloads, create_bc_session

CONTRACTS={
    "AUD":{"code":"A6","cycle":"HMUZ","tick_date":"2009-11-24"},
    "GOLD": {"code": "GC", "cycle": "GJMQVZ", "tick_date": "2008-05-04"}
}

session = create_bc_session(config_obj=dict(
    barchart_username="user@domain.com",
    barchart_password = "s3cr3t_321")
)

get_barchart_downloads(
    session,
    contract_map=CONTRACTS,
    save_directory='/home/user/contract_data',
    start_year=2020,
    end_year=2021
)
```

The code above would: 
* for the CME Australian Dollar future, get hourly OHLCV data for the Mar, Jun, Sep and Dec 2020 contracts
* download in CSV format
* save with filenames AUD_20200300.csv, AUD_20200600.csv, AUD_20200900.csv, AUD_20201200.csv into the specified directory
* for COMEX Gold, get Feb, Apr, Jun, Aug, Oct, and Dec data, with filenames like GOLD_20200200.csv etc

Features:
* Designed to be run once a day by a scheduler
* the script handles skips contracts already downloaded
* by default gets 120 days of data per contract, override possible per instrument
* dry run mode to check setup
* there is logic to switch to daily data when hourly is not available
* you must be a registered user. Paid subscribers get 100 downloads a day, otherwise 5

