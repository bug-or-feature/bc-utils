import os
import logging
from bcutils.bc_utils import create_bc_session, get_barchart_downloads, _env

logging.basicConfig(level=logging.INFO)


def do_downloads_dry_run():
    get_barchart_downloads(
        create_bc_session(config_obj=_env()),
        contract_map={
            "FTSE100": {"code": "X", "cycle": "HMUZ", "tick_date": "2002-01-01"}
        },
        save_dir=os.getcwd(),
        start_year=2020,
        end_year=2021,
        dry_run=True,
        do_daily=False,
    )


def do_downloads_dry_run_no_tick_date():
    get_barchart_downloads(
        create_bc_session(config_obj=_env()),
        contract_map={
            "FTSE100": {"code": "X", "cycle": "HMUZ", "tick_date": "2023-01-01"}
        },
        save_dir=os.getcwd(),
        start_year=2020,
        end_year=2021,
        dry_run=True,
        do_daily=False,
    )


if __name__ == "__main__":
    # do_downloads_dry_run()
    do_downloads_dry_run_no_tick_date()
