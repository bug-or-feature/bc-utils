import os
import logging
from bcutils.bc_utils import get_barchart_downloads, create_bc_session

logging.basicConfig(level=logging.INFO)

CONTRACTS = {
    "AUD": {"code": "A6", "cycle": "HMUZ", "tick_date": "2009-11-24"},
    "GOLD": {"code": "GC", "cycle": "GJMQVZ", "tick_date": "2008-05-04"},
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
    save_dir=os.getcwd(),
    start_year=2020,
    end_year=2021,
)
