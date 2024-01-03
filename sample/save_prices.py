import os
import logging
from bcutils.bc_utils import (
    create_bc_session,
    save_prices_for_contract,
    _build_save_path,
    _get_contract_month_year,
    _get_start_end_dates,
    _env,
)

logging.basicConfig(level=logging.INFO)


def save_hourly(instr_code, contract_key):
    month, year = _get_contract_month_year(contract_key)

    save_path = _build_save_path(instr_code, month, year, "Hour", os.getcwd())
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
    month, year = _get_contract_month_year(contract_key)

    save_path = _build_save_path(instr_code, month, year, "Day", os.getcwd())
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


if __name__ == "__main__":
    save_hourly("AUD", "A6H20")
    # save_daily("AUD", "A6H20")
    # save_hourly("CHFJPY", "UPU14")
