import json
from copy import copy
from bcutils.config import CONTRACT_MAP
from bcutils.bc_utils import create_bc_session, _get_exchange_for_code


def exchanges():
    contract_map = CONTRACT_MAP
    session = create_bc_session({}, do_login=False)
    exchange_map = {}

    for instr in contract_map.keys():
        config_obj = contract_map[instr]
        futures_code = config_obj["code"]
        rollcycle = config_obj["cycle"]

        contract_key = f"{futures_code}{rollcycle[:1]}24"
        print(contract_key)
        exchange = _get_exchange_for_code(session, contract_key)
        print(f"Exchange for {instr} ({futures_code}): {exchange}")

        updated_config = copy(config_obj)
        updated_config["exchange"] = exchange
        if "tick_date" in updated_config:
            del updated_config["tick_date"]
        contract_map[instr] = updated_config

        if exchange not in exchange_map:
            exchange_map[exchange] = {
                "tick_date": "2000-01-01",
                "eod_date": "1990-01-01",
            }

    print(json.dumps(exchange_map))


if __name__ == "__main__":
    exchanges()
