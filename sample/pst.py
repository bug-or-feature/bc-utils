import logging
from yaml import load, FullLoader

from bcutils.bc_utils import (
    create_bc_session,
    get_barchart_downloads,
    update_barchart_downloads,
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


def update_with_config():
    # run an update session, with config picked up from the passed file
    # See /sample/private_config_sample.yaml
    config = load_config("./private_config.yaml")
    instr_list = config["barchart_update_list"]
    save_dir = config["barchart_path"]
    dry_run = config["barchart_dry_run"]
    for code in instr_list:
        update_barchart_downloads(instr_code=code, save_dir=save_dir, dry_run=dry_run)


def load_config(config_path):
    config_stream = open(config_path, "r")
    return load(config_stream, Loader=FullLoader)


if __name__ == "__main__":
    download_with_config()
    # update_with_config()
