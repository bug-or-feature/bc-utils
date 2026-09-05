import logging
from yaml import load, FullLoader

from bcutils.bc_utils import (
    create_bc_session,
    get_barchart_downloads,
)

logging.basicConfig(level=logging.INFO)


def download_with_config():
    # run a download session, with config picked up from the passed file
    # See /sample/private_config_sample.yaml
    config = load_config("./private_config.yaml")
    get_barchart_downloads(
        create_bc_session(config),
        instr_list=config["barchart_download_list"],
        start_year=config["barchart_start_year"],
        end_year=config["barchart_end_year"],
        save_dir=config["barchart_path"],
        do_daily=config["barchart_do_daily"],
        dry_run=config["barchart_dry_run"],
    )


def load_config(config_path):
    config_stream = open(config_path, "r")
    return load(config_stream, Loader=FullLoader)


if __name__ == "__main__":
    download_with_config()
