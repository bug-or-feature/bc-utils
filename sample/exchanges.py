import asyncio
import json
from copy import copy

from patchright.async_api import async_playwright

from bcutils.config import CONTRACT_MAP
from bcutils.bc_utils import (
    create_bc_session,
    _get_exchange_for_code_async,
    _launch_barchart_browser,
    _login_async,
    _pause,
    _DEFAULT_AUTH_DIR,
    _env,
)


async def _exchanges_async(contract_map: dict | None = None):
    resolved_map: dict = CONTRACT_MAP if contract_map is None else contract_map
    session = create_bc_session(config_obj=_env())
    exchange_map = {}
    downloads_dir = _DEFAULT_AUTH_DIR.parent / "downloads"

    async with async_playwright() as playwright:
        context, human = await _launch_barchart_browser(
            playwright, _DEFAULT_AUTH_DIR, downloads_dir, False
        )
        try:
            await _login_async(human, session.username, session.password)

            for instr in resolved_map.keys():
                config_obj: dict = resolved_map[instr]
                futures_code = config_obj["code"]
                rollcycle = config_obj["cycle"]

                contract_key = f"{futures_code}{rollcycle[:1]}24"
                print(contract_key)
                exchange = await _get_exchange_for_code_async(human, contract_key)
                print(f"Exchange for {instr} ({futures_code}): {exchange}")

                updated_config = copy(config_obj)
                updated_config["exchange"] = exchange
                if "tick_date" in updated_config:
                    del updated_config["tick_date"]
                resolved_map[instr] = updated_config

                if exchange not in exchange_map:
                    exchange_map[exchange] = {
                        "tick_date": "2000-01-01",
                        "eod_date": "1990-01-01",
                    }

                await _pause(human, 1, 3)
        finally:
            await context.close()

    print(json.dumps(exchange_map))


def exchanges(contract_map: dict | None = None):
    asyncio.run(_exchanges_async(contract_map))


if __name__ == "__main__":
    exchanges(
        {
            "FTSE100": {"code": "X", "cycle": "HMUZ"},
            "AUD": {"code": "A6", "cycle": "HMUZ"},
        }
    )
