import asyncio
import calendar
import enum
import io
import json
import logging
import os
import os.path
import pytz
import random
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import cycle
from pathlib import Path
from random import randint

import pandas as pd
from bs4 import BeautifulSoup
from humanization import Humanization, HumanizationConfig
from loguru import logger as _loguru_logger
from patchright.async_api import TimeoutError as PlaywrightTimeoutError
from patchright.async_api import async_playwright

from bcutils.config import CONTRACT_MAP, EXCHANGES

# redirect humanization logging to stdout instead.
_loguru_logger.remove()
_loguru_logger.add(sys.stdout)

logger = logging.getLogger(__name__)


class HistoricalDataResult(enum.Enum):
    NONE = 1
    OK = 2
    EXISTS = 3
    EXCEED = 4
    INSUFFICIENT = 5


class Resolution(enum.Enum):
    Day = (1, "daily")
    Hour = (2, "hourly")

    def __init__(self, value, adjective):
        self._value_ = value
        self._adjective_ = adjective

    @property
    def adj(self):
        return self._adjective_


class BCException(Exception):
    pass


class IntegrityException(Exception):
    pass


class RecentUpdateException(Exception):
    pass


class EmptyDataException(Exception):
    pass


MONTH_LIST = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
BARCHART_URL = "https://www.barchart.com/"

_DEFAULT_AUTH_DIR = Path.home() / ".bc_utils" / "auth"

_HUMANIZATION_CONFIG = HumanizationConfig(
    fast=True,
    humanize=True,
    characters_per_minute=500,
    backspace_cpm=400,
    timeout=10000,
    stealth_mode=True,
)


@dataclass
class BarchartSession:
    """
    Holds credentials for a lazy, browser-driven Barchart login.
    """

    username: str
    password: str


def _disable_password_manager(user_data_dir: Path) -> None:
    """Pre-seed the profile so "Save password?" bubble never appears"""
    profile_dir = user_data_dir / "Default"
    profile_dir.mkdir(parents=True, exist_ok=True)
    prefs_path = profile_dir / "Preferences"

    prefs = {}
    if prefs_path.exists():
        try:
            prefs = json.loads(prefs_path.read_text())
        except (json.JSONDecodeError, OSError):
            prefs = {}

    prefs["credentials_enable_service"] = False
    prefs.setdefault("profile", {})
    prefs["profile"]["password_manager_enabled"] = False

    prefs_path.write_text(json.dumps(prefs))


async def _pause(
    human: Humanization, base_min: float = 0.2, base_max: float = 1.0
) -> None:
    # wait a randomised duration, jittering base_min/base_max themselves each call
    jitter = random.uniform(0.6, 1.6)
    min_sec = base_min * jitter
    max_sec = max(min_sec + 0.1, base_max * jitter * random.uniform(1.0, 1.4))
    await human.human_wait(min_sec=min_sec, max_sec=max_sec)


def _make_download_response_handler(allowance_slot: dict):
    """Capture the allowance-check response (success/count, or an error when
    the daily download limit is reached)"""

    async def handler(response):
        if (
            response.request.method != "POST"
            or response.request.url != BARCHART_URL + "my/download"
        ):
            return
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            return
        try:
            body = await response.json()
        except Exception:  # skipcq broad by design
            return
        allowance_slot.update(body)

    return handler


async def _launch_barchart_browser(
    playwright, auth_dir: Path, downloads_dir: Path, headless: bool
):
    _disable_password_manager(auth_dir)
    downloads_dir.mkdir(parents=True, exist_ok=True)

    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(auth_dir),
        args=["--disable-blink-features=AutomationControlled"],
        headless=headless,
        no_viewport=True,
        accept_downloads=True,
        downloads_path=str(downloads_dir),
    )
    page = await context.new_page()
    human = Humanization(page, _HUMANIZATION_CONFIG)
    return context, human


async def _login_async(human: Humanization, username: str, password: str) -> None:
    logger.info("step: goto barchart.com")
    await human.page.goto(BARCHART_URL)
    await _pause(human)

    allow_all = human.page.get_by_role("button", name="Allow all")
    if await allow_all.count() > 0:
        logger.info("step: accept cookie banner")
        await human.hover_at(allow_all)
        await _pause(human)
        await human.click_at(allow_all)
        await _pause(human)
    else:
        logger.info("step: cookie banner not present, skipping")

    login_link = human.page.get_by_role("link", name="LOGIN", description="LOGIN")
    if await login_link.count() > 0:
        logger.info("step: click LOGIN link")
        await human.hover_at(login_link)
        await _pause(human)
        await human.click_at(login_link)
        await _pause(human)
    else:
        logger.info("step: LOGIN link not present, skipping")

    email_box = human.page.get_by_role("textbox", name="Login with Email")
    if await email_box.count() > 0:
        logger.info("step: fill email")
        await human.type_at(email_box, username)
        await _pause(human)

    password_box = human.page.get_by_role("textbox", name="Password")
    if await password_box.count() > 0:
        logger.info("step: fill password")
        await human.type_at(password_box, password)
        await _pause(human)

    login_button = human.page.get_by_role("button", name="Login")
    if await login_button.count() > 0:
        logger.info("step: submit login")
        await human.hover_at(login_button)
        await _pause(human)
        await human.click_at(login_button)
        await _pause(human)

    # if the email field is still there after an attempted submit, login
    # didn't take - matches the old code's BCException on a failed login
    if await email_box.count() > 0:
        raise BCException("Invalid credentials")


def create_bc_session(config_obj: dict) -> BarchartSession:
    """
    Validate credentials and return a BarchartSession.
    Args:
        config_obj: dict containing Barchart credentials
    Returns:
        A BarchartSession instance
    Raises:
        BCException: if credentials are missing
    """
    if "barchart_username" not in config_obj or "barchart_password" not in config_obj:
        raise BCException("Missing credentials")
    return BarchartSession(
        username=config_obj["barchart_username"],
        password=config_obj["barchart_password"],
    )


def _normalize_downloaded_csv(save_path: str, res: Resolution) -> int:
    """fix column names, date format, remove footer; returns row count"""
    dateformat = "%Y-%m-%d" if res == Resolution.Day else "%Y-%m-%d %H:%M"

    df = pd.read_csv(save_path, skipfooter=1, engine="python")
    df["Time"] = pd.to_datetime(df["Time"], format=dateformat)
    df.set_index("Time", inplace=True)
    df.index = df.index.tz_localize(tz="US/Central").tz_convert("UTC")
    df = df.rename(columns={"Latest": "Close"})
    df = df[["Open", "High", "Low", "Close", "Volume"]]

    df.to_csv(save_path, date_format="%Y-%m-%dT%H:%M:%S%z")
    return len(df)


async def _save_prices_for_contract_async(
    human: Humanization,
    contract: str,
    save_path: str,
    start_date: datetime,
    end_date: datetime,
    dry_run: bool,
    allowance_slot: dict,
    instr_config: dict | None = None,
    insufficient_check_margin_days: int = 365,
):
    res = _get_resolution(save_path)

    # do we have this file already?
    if os.path.isfile(save_path):
        logger.info(
            f"{res.adj} data for contract '{contract}' already downloaded "
            f"({save_path}) - skipping"
        )
        return HistoricalDataResult.EXISTS

    if instr_config is not None and _near_available_res_boundary(
        res, start_date, instr_config, insufficient_check_margin_days
    ):
        logger.info(f"step: near availability boundary, checking '{contract}'")
        if await _insufficient_data_async(human, contract, res):
            logger.info(f"Insufficient {res.adj} data for '{contract}' - skipping")
            return HistoricalDataResult.INSUFFICIENT

    logger.info(
        f"getting historic {res.adj} prices for contract '{contract}', "
        f"from {start_date.strftime('%Y-%m-%d')} "
        f"to {end_date.strftime('%Y-%m-%d')}"
    )

    try:
        url = f"{BARCHART_URL}futures/quotes/{contract}/historical-download"
        logger.info(f"step: goto historical-download page for {contract}")
        response = await human.page.goto(url)
        if response is None or response.status != 200:
            logger.info(f"No downloadable data found for contract '{contract}'")
            return HistoricalDataResult.NONE

        # give the page's own JS time to finish hydrating
        await _pause(human)

        if dry_run:
            logger.info(f"Not downloading {contract}, dry_run")
            return HistoricalDataResult.OK

        select_value = "string:daily" if res == Resolution.Day else "string:minutes"

        allowance_slot.clear()
        logger.info(f"step: select frequency ({select_value})")
        frequency_select = human.page.get_by_label("Select frequency")
        max_attempts = 3
        for attempt in range(max_attempts):
            await frequency_select.select_option(select_value)
            await _pause(human)
            actual_value = await frequency_select.input_value()
            if actual_value == select_value:
                break
            logger.warning(
                f"frequency select shows '{actual_value}', expected "
                f"'{select_value}' for '{contract}' - retrying "
                f"(attempt {attempt + 1}/{max_attempts})"
            )
        else:
            logger.error(
                f"frequency select never settled on '{select_value}' for "
                f"'{contract}' after {max_attempts} attempts - aborting"
            )
            return HistoricalDataResult.NONE

        if res == Resolution.Hour:
            logger.info("step: set intraday minutes to 60")
            minutes_box = human.page.get_by_role(
                "spinbutton", name="Enter intraday minutes"
            )
            await minutes_box.fill("")
            await human.type_at(minutes_box, "60")
            await _pause(human)

        logger.info("step: select ordering (asc)")
        await human.page.get_by_label("Select Ordering").select_option("asc")
        await _pause(human)

        # set start date
        logger.info(f"step: set start date ({start_date.strftime('%m/%d/%Y')})")
        start_box = human.page.get_by_role("textbox", name="Start Date")
        await start_box.click()
        await _pause(human)
        await human.page.get_by_role("button", name="Clear").click()
        await _pause(human)
        await human.type_at(start_box, start_date.strftime("%m/%d/%Y"))
        await human.page.keyboard.press("Escape")
        await _pause(human)

        # set end date
        logger.info(f"step: set end date ({end_date.strftime('%m/%d/%Y')})")
        end_box = human.page.get_by_role("textbox", name="End Date")
        await end_box.click()
        await _pause(human)
        await human.page.get_by_role("button", name="Clear").click()
        await _pause(human)
        await human.type_at(end_box, end_date.strftime("%m/%d/%Y"))
        await human.page.keyboard.press("Escape")
        await _pause(human)

        logger.info(f"step: click Download for '{contract}'")
        download_link = human.page.get_by_text("Download", exact=True).first
        await human.hover_at(download_link)
        try:
            async with human.page.expect_download(timeout=15000) as download_info:
                await human.click_at(download_link)
            download = await download_info.value
        except PlaywrightTimeoutError:
            if allowance_slot.get("error") is not None:
                logger.info(f"Max daily download reached for '{contract}'")
                return HistoricalDataResult.EXCEED
            raise

        if allowance_slot.get("error") is not None:
            logger.info(f"Max daily download reached for '{contract}'")
            return HistoricalDataResult.EXCEED

        failure = await download.failure()
        if failure:
            logger.info(f"Barchart data problem for '{contract}', not writing")
            return HistoricalDataResult.OK

        logger.info(f"step: saving download to {save_path}")
        await download.save_as(save_path)
        row_count = _normalize_downloaded_csv(save_path, res)
        if row_count < 30:
            os.remove(save_path)
            logger.info(f"Insufficient {res.adj} data for '{contract}' - skipping")
            return HistoricalDataResult.INSUFFICIENT

        logger.info(
            f"Finished getting Barchart historic {res.adj} prices for {contract}"
        )
        return HistoricalDataResult.OK

    except Exception as e:  # skipcq broad by design
        logger.error(f"Error {e}")


async def _save_prices_for_contract_standalone_async(
    session: BarchartSession,
    contract: str,
    save_path: str,
    start_date: datetime,
    end_date: datetime,
    dry_run: bool,
    headless: bool,
    auth_dir: str | None = None,
):
    auth_dir_path = Path(auth_dir) if auth_dir else _DEFAULT_AUTH_DIR
    downloads_dir = Path(save_path).resolve().parent

    async with async_playwright() as playwright:
        context, human = await _launch_barchart_browser(
            playwright, auth_dir_path, downloads_dir, headless
        )
        try:
            await _login_async(human, session.username, session.password)

            allowance_slot: dict = {}
            human.page.on("response", _make_download_response_handler(allowance_slot))

            return await _save_prices_for_contract_async(
                human,
                contract,
                save_path,
                start_date,
                end_date,
                dry_run,
                allowance_slot,
            )
        finally:
            await context.close()


def save_prices_for_contract(
    session: BarchartSession,
    contract: str,
    save_path: str,
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
    headless: bool = False,
    auth_dir: str | None = None,
):
    """
    Save prices for an individual futures contract.

    Args:
        session: a BarchartSession instance
        contract: Barchart style contract identifier, eg GCH24 for March 2024 Gold
        save_path: full path where price file will be saved
        start_date: start date
        end_date: end date
        dry_run: if True, provides useful diagnostic info but does not execute
        headless: if True, run the browser without a visible window. Requires a
            display (e.g. Xvfb) on headless servers. Defaults to False
        auth_dir: directory to persist the browser's login/session state across
            runs. Defaults to ~/.bc_utils/auth

    Returns:
        A HistoricalDataResult instance, representing the result of the operation
    """
    return asyncio.run(
        _save_prices_for_contract_standalone_async(
            session,
            contract,
            save_path,
            start_date,
            end_date,
            dry_run,
            headless,
            auth_dir,
        )
    )


async def _get_barchart_downloads_async(
    session: BarchartSession,
    contract_map,
    contract_list,
    instr_list,
    save_dir,
    start_year,
    end_year,
    dry_run,
    do_daily,
    pause_between_downloads,
    default_day_count,
    headless,
    auth_dir,
    insufficient_check_margin_days,
):
    if contract_map is None:
        contract_map = CONTRACT_MAP

    inv_contract_map = _build_inverse_map(contract_map)

    max_exceeded = False

    if contract_list is None:
        contract_list = _build_contract_list(
            start_year, end_year, instr_list=instr_list, contract_map=contract_map
        )

    auth_dir_path = Path(auth_dir) if auth_dir else _DEFAULT_AUTH_DIR
    downloads_dir = Path(save_dir) if save_dir else Path(os.getcwd())

    async with async_playwright() as playwright:
        context, human = await _launch_barchart_browser(
            playwright, auth_dir_path, downloads_dir, headless
        )
        try:
            await _login_async(human, session.username, session.password)

            allowance_slot: dict = {}
            human.page.on("response", _make_download_response_handler(allowance_slot))

            for contract in contract_list:
                if max_exceeded:
                    break

                for resolution in Resolution if do_daily else [Resolution.Hour]:
                    # work out instrument code and get config
                    market_code = contract[: len(contract) - 3]
                    instr_code = inv_contract_map[market_code.upper()]
                    instr_config = contract_map[instr_code]

                    # get contract month and year
                    month, year = _get_contract_month_year(contract)

                    # build save path
                    save_path = _build_save_path(
                        instr_code, month, year, resolution, save_dir
                    )

                    # calculate date range
                    start_date, end_date = _get_start_end_dates(
                        month,
                        year,
                        instr_config,
                        default_day_count=default_day_count,
                    )

                    if _before_available_res(resolution, start_date, instr_config):
                        date_type = "tick" if resolution == Resolution.Hour else "EOD"
                        logger.info(
                            f"{resolution.adj} prices for {contract} starting "
                            f"{start_date.strftime('%Y-%m-%d')} is before configured "
                            f"{date_type} date - skipping"
                        )
                        continue

                    # download and save
                    result = await _save_prices_for_contract_async(
                        human,
                        contract,
                        save_path,
                        start_date,
                        end_date,
                        dry_run,
                        allowance_slot,
                        instr_config,
                        insufficient_check_margin_days,
                    )

                    if result in [
                        HistoricalDataResult.EXISTS,
                        HistoricalDataResult.NONE,
                        HistoricalDataResult.INSUFFICIENT,
                    ]:
                        continue
                    elif result == HistoricalDataResult.EXCEED:
                        logger.info("Max daily download reached, aborting")
                        max_exceeded = True
                        break
                    else:
                        if pause_between_downloads:
                            # cursory attempt to not appear like a bot
                            await asyncio.sleep(0 if dry_run else randint(7, 15))
        finally:
            await context.close()


def get_barchart_downloads(
    session: BarchartSession,
    contract_map: dict | None = None,
    contract_list: list | None = None,
    instr_list: list | None = None,
    save_dir: str | None = None,
    start_year: int = 1950,
    end_year: int = 2025,
    dry_run: bool = False,
    do_daily: bool = True,
    pause_between_downloads: bool = True,
    default_day_count: int = 400,
    headless: bool = False,
    auth_dir: str | None = None,
    insufficient_check_margin_days: int = 365,
):
    """
    Run a download session, performing as many contract downloads as possible, given
    the config, parameters, existing files, and available daily allowance.

    Args:
        session: a BarchartSession instance
        contract_map: dict containing instrument config
        contract_list: optional list of Barchart contract IDs we want to download in
            this run. If provided, `start_year` and `start_year` are ignored. If not
            provided, a list will be created based on the parameters. See
            `_build_contract_list()`
        instr_list: list of instrument codes (eg GOLD, AUD) we want to download in this
            run
        save_dir: full path to the directory where we want downloaded files to be saved
        start_year: start year as an int
        end_year: end year as an int
        dry_run: if True, provides useful diagnostic info but does not execute
        do_daily: if True, download daily as well as hourly price files
        pause_between_downloads: if True, wait a random short period between downloads
        default_day_count: default number of days of data to download
        headless: if True, run the browser without a visible window. Requires a
            display (e.g. Xvfb) on headless servers. Defaults to False
        auth_dir: directory to persist the browser's login/session state across
            runs. Defaults to ~/.bc_utils/auth
        insufficient_check_margin_days: contracts expiring within this many days of
            the exchange's published tick_date/eod_date get a live insufficient-data
            check before downloading, since those published limits are often
            inaccurate
    """
    try:
        asyncio.run(
            _get_barchart_downloads_async(
                session,
                contract_map,
                contract_list,
                instr_list,
                save_dir,
                start_year,
                end_year,
                dry_run,
                do_daily,
                pause_between_downloads,
                default_day_count,
                headless,
                auth_dir,
                insufficient_check_margin_days,
            )
        )
    except Exception as e:  # skipcq broad by design
        logger.error(f"Error {e}")
        traceback.print_exc()


async def _update_barchart_downloads_async(
    instr_code,
    contract_map,
    save_dir,
    days_ago,
    dry_run,
    split_freq,
    session: BarchartSession,
    headless,
    auth_dir,
):
    if contract_map is None:
        contract_map = CONTRACT_MAP

    from_date = datetime.now() - timedelta(days=days_ago)

    logger.info(f"Updating contract prices for {instr_code}")

    check_integrity_list = []

    file_names = _get_filenames(instr_code, save_dir, split_freq)

    auth_dir_path = Path(auth_dir) if auth_dir else _DEFAULT_AUTH_DIR
    downloads_dir = Path(save_dir) if save_dir else Path(os.getcwd())

    async with async_playwright() as playwright:
        context, human = await _launch_barchart_browser(
            playwright, auth_dir_path, downloads_dir, headless
        )
        try:
            await _login_async(human, session.username, session.password)

            for file in file_names:
                instr_code = _instr_code_from_file_name(file, split_freq=split_freq)
                if split_freq:
                    res = _res_from_file_name(file)
                else:
                    res = None
                contract_date = _contract_date_from_file_name(file)
                contract_id = _get_barchart_id(
                    instr_code, contract_date.year, contract_date.month
                )

                if contract_date > from_date:
                    if dry_run:
                        print(
                            f"DRY RUN: would update contract {contract_id}, "
                            f"file {file}"
                        )
                    else:
                        try:
                            await _update_barchart_contract_file_async(
                                human, contract_map, save_dir, contract_id, res
                            )
                        except IntegrityException:
                            logger.error(
                                f"File index problem with {file}, please check"
                            )
                            check_integrity_list.append(file)
                        except RecentUpdateException:
                            logger.warning(f"Skipping {contract_id}, recently updated")
                        except EmptyDataException:
                            logger.info(f"Empty data for {contract_id}")
        finally:
            await context.close()

    if len(check_integrity_list) > 0:
        print(f"These files have integrity problems: {check_integrity_list}")


def update_barchart_downloads(
    instr_code: str = "GOLD",
    contract_map: dict | None = None,
    save_dir: str | None = None,
    days_ago: int = 360,
    dry_run: bool = False,
    split_freq: bool = True,
    session: BarchartSession | None = None,
    headless: bool = False,
    auth_dir: str | None = None,
):
    """
    Update recent previously downloaded files for an instrument.

    Considers previously downloaded contract files where the contract date is more
    recent than `days_ago`. For each file, will update it with any new price data rows,
    given the existing resolution.

    Args:
        instr_code: instrument code (eg GOLD)
        contract_map: dict containing instrument config
        save_dir: full path to the directory where previously downloaded files are
            located
        days_ago: how many days to look back. A file's contract date is assumed to be
            the 1st of the month. So GCH23 would be 1st March 2023
        dry_run: if True, provides useful diagnostic info but does not execute
        split_freq: True if we are expecting to find split frequency files
        session: a BarchartSession instance from create_bc_session(). If not
            provided, one is built from BARCHART_USERNAME/BARCHART_PASSWORD
            environment variables
        headless: if True, run the browser without a visible window. Requires a
            display (e.g. Xvfb) on headless servers. Defaults to False
        auth_dir: directory to persist the browser's login/session state across
            runs. Defaults to ~/.bc_utils/auth
    """
    if session is None:
        session = create_bc_session(config_obj=_env())

    asyncio.run(
        _update_barchart_downloads_async(
            instr_code,
            contract_map,
            save_dir,
            days_ago,
            dry_run,
            split_freq,
            session,
            headless,
            auth_dir,
        )
    )


def _get_filenames(instr_code, save_dir, split_freq: bool = True):
    file_names = []
    if split_freq:
        for res in Resolution:
            regex = re.compile("^" + res.name + "_" + instr_code + "_[0-9]{8}.csv")
            file_names.extend([fn for fn in os.listdir(save_dir) if regex.match(fn)])
    else:
        regex = re.compile("^" + instr_code + "_[0-9]{8}.csv")
        file_names.extend([fn for fn in os.listdir(save_dir) if regex.match(fn)])

    return file_names


async def _update_barchart_contract_file_async(
    human: Humanization,
    contract_map: dict,
    path: str,
    contract_id: str,
    res: Resolution,
):
    inv_contract_map = _build_inverse_map(contract_map)

    file = _filename_from_barchart_id(contract_id, inv_contract_map, res)
    instr_code = _instr_code_from_file_name(file, res is not None)

    now = datetime.now().astimezone(tz=pytz.utc)

    input_path = f"{path}/{file}"
    logger.info(f"Starting update for {input_path}...")

    existing = pd.read_csv(input_path)
    existing["Time"] = pd.to_datetime(existing["Time"], format="%Y-%m-%dT%H:%M:%S%z")
    try:
        existing.set_index("Time", inplace=True, verify_integrity=True)
        last_index_date = existing.index[-1]
    except ValueError:
        raise IntegrityException(f"Index problem with {file}, needs manual check")

    if (now - last_index_date).days < 4:
        raise RecentUpdateException(f"Skipping {file}, recently updated")

    logger.info(
        f"Instrument: {instr_code}, contract: {contract_id}, "
        f"last entry: {last_index_date}"
    )

    update = await _get_historical_prices_for_contract_async(human, contract_id, res)
    if res == Resolution.Hour:
        start = last_index_date + timedelta(hours=1)
    else:
        start = last_index_date + timedelta(hours=25)

    if update is not None:
        logger.info(
            f"Adding new rows from {start.strftime('%Y-%m-%d')} to "
            f"{now.strftime('%Y-%m-%d')}"
        )
        update = update[start:]

        try:
            final = pd.concat([existing, update], verify_integrity=True)
            output_path = f"{path}/{file}"
            final.to_csv(output_path, date_format="%Y-%m-%dT%H:%M:%S%z")
        except Exception as ex:
            logger.warning(f"Problem with {file}: {ex}")
    else:
        raise EmptyDataException(f"Empty data for {contract_id}")


async def _update_barchart_contract_file_standalone_async(
    session: BarchartSession,
    contract_map: dict,
    path: str,
    contract_id: str,
    res: Resolution,
    headless: bool,
    auth_dir: str | None = None,
):
    auth_dir_path = Path(auth_dir) if auth_dir else _DEFAULT_AUTH_DIR
    downloads_dir = Path(path)

    async with async_playwright() as playwright:
        context, human = await _launch_barchart_browser(
            playwright, auth_dir_path, downloads_dir, headless
        )
        try:
            await _login_async(human, session.username, session.password)
            return await _update_barchart_contract_file_async(
                human, contract_map, path, contract_id, res
            )
        finally:
            await context.close()


def update_barchart_contract_file(
    session: BarchartSession,
    contract_map: dict,
    path: str,
    contract_id: str,
    res: Resolution,
    headless: bool = False,
    auth_dir: str | None = None,
):
    """
    Update a previously downloaded contract price file.

    Args:
        session: a BarchartSession instance
        contract_map: dict containing instrument config
        path: full path to the directory where previously downloaded files are located
        contract_id: Barchart style contract identifier, eg GCH24 for March 2024 Gold
        res: Resolution.Hour or Resolution.Day
        headless: if True, run the browser without a visible window. Requires a
            display (e.g. Xvfb) on headless servers. Defaults to False
        auth_dir: directory to persist the browser's login/session state across
            runs. Defaults to ~/.bc_utils/auth
    Raises:
        IntegrityException: raised if a problem is encountered when trying to set the
            datetime column as index
        RecentUpdateException: raised if the file has been recently updated
        EmptyDataException: raised if the update contains no data
    """
    return asyncio.run(
        _update_barchart_contract_file_standalone_async(
            session, contract_map, path, contract_id, res, headless, auth_dir
        )
    )


def _historical_prices_predicate(resolution: Resolution):
    # build a page.expect_response() predicate
    if resolution == Resolution.Day:
        url_prefix = BARCHART_URL + "proxies/timeseries/historical/queryeod.ashx"
        required = ("volume=contract", "data=daily")
    else:
        url_prefix = BARCHART_URL + "proxies/timeseries/historical/queryminutes.ashx"
        required = ("volume=contract", "interval=60")

    def predicate(response) -> bool:
        return (
            response.request.method == "GET"
            and response.request.url.startswith(url_prefix)
            and all(token in response.request.url for token in required)
        )

    return predicate


async def _get_historical_prices_for_contract_async(
    human: Humanization, contract_id: str, resolution: Resolution = Resolution.Day
) -> pd.DataFrame:
    if not contract_id:
        raise BCException("contract_id is required")

    try:
        chart_url = f"{BARCHART_URL}futures/quotes/{contract_id}/interactive-chart"
        resolution_label = "Daily" if resolution == Resolution.Day else "1 Hour"
        predicate = _historical_prices_predicate(resolution)

        logger.info(f"step: goto interactive-chart page for {contract_id}")
        async with human.page.expect_response(
            predicate, timeout=20000
        ) as response_info:
            await human.page.goto(chart_url)
            await _pause(human)

            logger.info("step: click Max")
            await human.page.get_by_role("button", name="Max").click()
            await _pause(human)

            logger.info(f"step: switch chart resolution to {resolution_label}")
            await human.page.locator("text-binding").nth(1).click()
            await _pause(human)
            await (
                human.page.locator("text-binding")
                .filter(has_text=re.compile(rf"^{re.escape(resolution_label)}$"))
                .click()
            )
        response = await response_info.value

        ratelimit = response.headers.get("x-ratelimit-remaining")
        if ratelimit is not None and int(ratelimit) <= 15:
            await asyncio.sleep(20)
        logger.info(
            f"GET {response.url} {contract_id}, {response.status}, "
            f"ratelimit {ratelimit}"
        )

        # read response into dataframe
        text = await response.text()
        iostr = io.StringIO(text)
        df = pd.read_csv(iostr, header=None)

        # convert to expected format
        price_data_as_df = _raw_barchart_data_to_df(df, bar_freq=resolution)

        if len(df) == 0:
            raise BCException(
                f"Zero length Barchart price data found for {contract_id}"
            )

        logger.debug(f"Latest price {df.index[-1]} with {resolution}")

        return price_data_as_df

    except Exception as ex:
        logger.error(f"Problem getting historical data: {ex}")
        raise BCException from ex


async def _insufficient_data_async(
    human: Humanization, contract: str, resolution: Resolution
) -> bool:
    try:
        df = await _get_historical_prices_for_contract_async(
            human, contract, resolution
        )
        logger.info(f"step: got {len(df)} rows for '{contract}'")
        return len(df) < 30
    except Exception:  # skipcq broad by design
        return True


def _build_contract_list(start_year, end_year, instr_list=None, contract_map=None):
    contracts_per_instrument = {}
    contract_list = []
    count = 0

    if contract_map is None:
        contract_map = CONTRACT_MAP

    if instr_list is None:
        instr_list = contract_map.keys()

    for instr in instr_list:
        config_obj = contract_map[instr]
        futures_code = config_obj["code"]
        if futures_code == "none":
            continue
        rollcycle = config_obj["cycle"]
        instrument_list = []

        for year in range(start_year, end_year):
            for month_code in list(rollcycle):
                instrument_list.append(
                    f"{futures_code}{month_code}{str(year)[len(str(year))-2:]}"
                )
        contracts_per_instrument[instr] = instrument_list
        logger.info(f"Adding {len(instrument_list)} contracts for {instr}")
        count = count + len(instrument_list)

    logger.info(f"Contract count: {count}")

    pool = cycle(contract_map.keys())

    # Count how many contracts are actually available to prevent infinite loops
    available_contracts = sum(
        len(contracts_per_instrument.get(instr, [])) for instr in contract_map.keys()
    )
    if available_contracts < count:
        logger.warning(
            f"Only {available_contracts} contracts available but count is set "
            f"to {count}. Adjusting count."
        )
        count = available_contracts

    while len(contract_list) < count:
        try:
            instr = next(pool)
        except StopIteration:
            logger.warning("Reached the end of the pool unexpectedly")
            continue
        if instr not in contracts_per_instrument:
            continue
        instr_list = contracts_per_instrument[instr]
        config_obj = contract_map[instr]
        rollcycle = config_obj["cycle"]
        if len(rollcycle) > 10:
            max_count = 3
        elif len(rollcycle) > 7:
            max_count = 2
        else:
            max_count = 1

        for _ in range(0, max_count):
            if len(instr_list) > 0:
                contract_list.append(instr_list.pop())

    # return ['CTH21', 'CTK21', 'CTN21', 'CTU21', 'CTZ21', 'CTH22']

    logger.info(f"Contract list: {contract_list}")
    return contract_list


def _build_inverse_map(contract_map):
    return {v["code"]: k for k, v in contract_map.items()}


def _before_available_res(resolution, start_date, instr_config):
    if "exchange" in instr_config:
        exch = instr_config["exchange"]
        if exch not in EXCHANGES:
            raise BCException(f"Missing exchange config for {exch}")
        exch_config = EXCHANGES[exch]
        tick_date = datetime.strptime(exch_config["tick_date"], "%Y-%m-%d")
        eod_date = datetime.strptime(exch_config["eod_date"], "%Y-%m-%d")

        if resolution == Resolution.Hour:
            return tick_date is not None and start_date < tick_date
        else:
            return eod_date is not None and start_date < eod_date
    else:
        raise BCException(f"No exchange specified for {instr_config['code']}")


def _near_available_res_boundary(resolution, start_date, instr_config, margin_days):
    # Barchart's published limits are often inaccurate.
    if "exchange" not in instr_config:
        raise BCException(f"No exchange specified for {instr_config['code']}")
    exch = instr_config["exchange"]
    if exch not in EXCHANGES:
        raise BCException(f"Missing exchange config for {exch}")
    exch_config = EXCHANGES[exch]
    limit_key = "tick_date" if resolution == Resolution.Hour else "eod_date"
    limit_date = datetime.strptime(exch_config[limit_key], "%Y-%m-%d")
    return start_date <= limit_date + timedelta(days=margin_days)


def _build_save_path(instr_code, month, year, res: Resolution, save_directory):
    if save_directory is None:
        download_dir = os.getcwd()
    else:
        download_dir = save_directory
    datecode = str(year) + "{0:02d}".format(month)
    filename = f"{res.name}_{instr_code}_{datecode}00.csv"
    save_path = f"{download_dir}/{filename}"
    return save_path


def _get_contract_month_year(contract):
    year_code = int(contract[len(contract) - 2 :])
    month_code = contract[len(contract) - 3]
    if year_code > 30:
        year = 1900 + year_code
    else:
        year = 2000 + year_code
    month = _month_from_contract_letter(month_code.upper())
    return month, year


def _get_start_end_dates(month, year, instr_config=None, default_day_count: int = 400):
    now = datetime.now()
    if instr_config and "days_count" in instr_config:
        day_count = instr_config["days_count"]
    else:
        day_count = default_day_count

    # we need to work out a date range for which we want the prices
    # for expired contracts the end date would be the expiry date;
    # for KISS sake, lets assume expiry is last date of contract month
    end_date = datetime(year, month, calendar.monthrange(year, month)[1])

    # but, if that end_date is in the future, then we may as well make it today...
    if now.date() < end_date.date():
        end_date = now

    # let's set start date at <day_count> days before end date
    day_count = timedelta(days=day_count)
    start_date = end_date - day_count

    return start_date, end_date


def _month_from_contract_letter(contract_letter):
    """
    Returns month number (1 is January) from contract letter

    :param contract_letter:
    :return:
    """
    try:
        month_number = MONTH_LIST.index(contract_letter)
    except ValueError:
        return None

    return month_number + 1


def _get_resolution(save_path):
    path_obj = Path(save_path)
    resol_str = path_obj.name.split("_")[0]
    try:
        return Resolution[resol_str]
    except KeyError:
        raise BCException(f"Unknown resolution: {resol_str}")


def _raw_barchart_data_to_df(
    price_data_raw: pd.DataFrame,
    bar_freq: Resolution = Resolution.Day,
) -> pd.DataFrame:
    if price_data_raw is None:
        logger.warning("No historical price data from Barchart")
        return pd.DataFrame([])

    if bar_freq == Resolution.Day:
        dateformat = "%Y-%m-%d"
        col_no = 1
        cols_to_remove = [0, 1, 7]
    else:
        dateformat = "%Y-%m-%d %H:%M"
        col_no = 0
        cols_to_remove = [0, 1]

    price_data_raw["Date"] = pd.to_datetime(price_data_raw[col_no], format=dateformat)
    price_data_raw.set_index("Date", inplace=True)
    price_data_raw.index = price_data_raw.index.tz_localize(tz="US/Central").tz_convert(
        "UTC"
    )
    price_data_raw.index.name = "Time"
    df = price_data_raw.drop(columns=cols_to_remove)
    df.columns = ["Open", "High", "Low", "Close", "Volume"]

    return df


def _get_barchart_id(instr, year, month):
    instr_config = CONTRACT_MAP[instr]
    bc_instr = instr_config["code"]
    month_code = MONTH_LIST[month - 1]
    year_sub = year - 2000 if year > 2000 else year - 1900
    bc_id = f"{bc_instr}{month_code}{year_sub}"
    return bc_id


def _instr_code_from_file_name(file_name, split_freq: bool = True):
    if split_freq:
        instr_code = file_name[file_name.find("_") + 1 : file_name.rfind("_")]
    else:
        instr_code = file_name[: file_name.rfind("_")]
    return instr_code


def _res_from_file_name(file_name):
    res_str = file_name[: file_name.find("_")]
    res = Resolution[res_str]
    return res


def _contract_date_from_file_name(file_name):
    date_str = file_name[-12:-4]
    logger.debug(f"file: {file_name}, date_str: {date_str}")
    contract_date = datetime.strptime(f"{date_str[:-2]}01", "%Y%m01")
    return contract_date


def _filename_from_barchart_id(contract_id, inv_map, res: Resolution | None):
    try:
        month, year = _get_contract_month_year(contract_id)
        market_code = contract_id[: len(contract_id) - 3]
        instrument = inv_map[market_code.upper()]
        datecode = str(year) + "{0:02d}".format(month)
        if res is None:
            filename = f"{instrument}_{datecode}00.csv"
        else:
            filename = f"{res.name}_{instrument}_{datecode}00.csv"
        return filename
    except Exception as ex:
        raise Exception(f"Problem creating filename: {ex}")


def _env():
    credentials = {
        "barchart_username": "BARCHART_USERNAME",
        "barchart_password": "BARCHART_PASSWORD",
    }
    barchart_config = {
        k: os.environ.get(v) for k, v in credentials.items() if v in os.environ
    }
    return barchart_config


async def _get_exchange_for_code_async(human: Humanization, contract_code: str):
    # scrape the overview page info table for the exchange name
    try:
        url = f"{BARCHART_URL}futures/quotes/{contract_code}/overview"
        response = await human.page.goto(url)
        if response is None:
            return None
        if response.status == 200:
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find(name="div", attrs={"class": "commodity-profile"})
            label = table.find(name="div", string="Exchange")  # type: ignore[union-attr]
            exchange_raw = label.next_sibling.next_sibling  # type: ignore[union-attr]
            exchange = exchange_raw.text.strip()  # type: ignore[union-attr]
            return exchange
        if response.status == 404:
            print(f"Barchart page for {contract_code} not found")

    except Exception as e:
        print("Error: %s" % e)
        return None


async def _get_exchange_for_code_standalone_async(
    session: BarchartSession,
    contract_code: str,
    headless: bool = False,
    auth_dir: str | None = None,
):
    auth_dir_path = Path(auth_dir) if auth_dir else _DEFAULT_AUTH_DIR
    downloads_dir = _DEFAULT_AUTH_DIR.parent / "downloads"

    async with async_playwright() as playwright:
        context, human = await _launch_barchart_browser(
            playwright, auth_dir_path, downloads_dir, headless
        )
        try:
            await _login_async(human, session.username, session.password)
            return await _get_exchange_for_code_async(human, contract_code)
        finally:
            await context.close()


def _get_exchange_for_code(
    session: BarchartSession,
    contract_code: str,
    headless: bool = False,
    auth_dir: str | None = None,
):
    """Get the exchange for the given Barchart code."""
    return asyncio.run(
        _get_exchange_for_code_standalone_async(
            session, contract_code, headless, auth_dir
        )
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    get_barchart_downloads(
        create_bc_session(config_obj=_env()),
        instr_list=["NZD"],
        start_year=2023,
        end_year=2024,
        save_dir="/home/user/barchart_data",
        do_daily=True,
        dry_run=False,
    )
