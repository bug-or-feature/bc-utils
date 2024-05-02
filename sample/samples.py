import os
import logging

from bcutils.bc_utils import (
    create_bc_session,
    get_barchart_downloads,
    save_prices_for_contract,
    update_barchart_downloads,
    update_barchart_contract_file,
    Resolution,
    _build_save_path,
    _get_contract_month_year,
    _get_start_end_dates,
    _build_inverse_map,
    _env,
)
from bcutils.migrate import migrate_to_multi_freq
from bcutils.config import CONTRACT_MAP

logging.basicConfig(level=logging.INFO)


def download_hourly():
    # get hourly FTSE100 prices for the Mar, Jun, Sep and Dec 2020 contracts
    # (in dry_run mode)
    get_barchart_downloads(
        create_bc_session(config_obj=_env()),
        contract_map={"FTSE100": {"code": "X", "cycle": "HMUZ", "exchange": "ICE"}},
        save_dir=os.getcwd(),
        start_year=2020,
        end_year=2021,
        dry_run=True,
        do_daily=False,
    )


def download_hourly_and_daily():
    # get hourly and daily FTSE100 prices for the Mar, Jun, Sep and Dec 2020 contracts
    # (in dry_run mode)
    get_barchart_downloads(
        create_bc_session(config_obj=_env()),
        contract_map={"FTSE100": {"code": "X", "cycle": "HMUZ", "exchange": "ICE"}},
        save_dir=os.getcwd(),
        start_year=2020,
        end_year=2021,
        dry_run=True,
        do_daily=True,
    )


def download_specific_contracts():
    # get hourly and daily FTSE100 prices for the Mar 2018, Jun 2019, Sep 2020 contracts
    # (in dry_run mode)
    get_barchart_downloads(
        create_bc_session(config_obj=_env()),
        contract_map={"FTSE100": {"code": "X", "cycle": "HMUZ", "exchange": "ICE"}},
        contract_list=["XH18", "XM19", "XU20"],
        save_dir=os.getcwd(),
        dry_run=True,
        do_daily=True,
    )


def save_hourly(instr_code, contract_key):
    # save hourly prices for the given instrument code (eg GOLD) and contract ID
    # (eg GCG23)
    month, year = _get_contract_month_year(contract_key)
    save_path = _build_save_path(instr_code, month, year, Resolution.Hour, os.getcwd())
    print(f"Save_path: {save_path}")

    start_date, end_date = _get_start_end_dates(month, year)

    result = save_prices_for_contract(
        create_bc_session(config_obj=_env()),
        contract_key,
        save_path,
        start_date,
        end_date,
    )

    print(f"Result: {result}")


def save_daily(instr_code, contract_key):
    # save daily prices for the given instrument code (eg GOLD) and contract ID
    # (eg GCG23)
    month, year = _get_contract_month_year(contract_key)
    save_path = _build_save_path(instr_code, month, year, Resolution.Day, os.getcwd())
    print(f"Save_path: {save_path}")

    start_date, end_date = _get_start_end_dates(month, year)

    result = save_prices_for_contract(
        create_bc_session(config_obj=_env()),
        contract_key,
        save_path,
        start_date,
        end_date,
    )

    print(f"Result: {result}")


def update_downloads():
    # update any hourly or daily AUD price files in the current working directory
    update_barchart_downloads(
        instr_code="AUD",
        contract_map={"AUD": {"code": "A6", "cycle": "HMUZ", "exchange": "CME"}},
        save_dir=os.getcwd(),
        dry_run=False,
    )


def update_hourly_file():
    # update the hourly AUD Mar 2024 price file in the current working directory
    contract_map = {"AUD": {"code": "A6", "cycle": "HMUZ", "exchange": "CME"}}
    update_barchart_contract_file(
        create_bc_session(config_obj=_env()),
        _build_inverse_map(contract_map),
        os.getcwd(),
        "A6H24",
        Resolution.Hour,
    )


def update_daily_file(contract_key, config=None):
    if config is None:
        config = CONTRACT_MAP
    update_barchart_contract_file(
        create_bc_session(config_obj=_env()),
        config,
        os.getcwd(),
        contract_key,
        Resolution.Day,
    )


def rename_files_with_new_format():
    # search the given directory for any price files with the old name format
    # (AUD_20230300.csv), and analyse the prices inside, renaming with the new format
    # (Day_AUD_20230300.csv or Hour_AUD_20230300.csv)
    migrate_to_multi_freq(
        "/home/user/prices/barchart",
        ["AUD"],
        dry_run=True,
    )


if __name__ == "__main__":
    download_hourly()
    # download_hourly_and_daily()
    # save_hourly("AUD", "A6H20")
    # save_daily("AUD", "A6H20")
    # save_hourly("CHFJPY", "UPU14")
    # update_downloads()
    # update_hourly_file()
    # update_daily_file("EPM24")
    # rename_files_with_new_format()
