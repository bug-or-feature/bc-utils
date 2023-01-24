import json
import enum
import os
import os.path
from itertools import cycle
import calendar
import logging
import io
import urllib.parse
from random import randint
import traceback
from datetime import datetime, timedelta
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd

from bcutils.config import CONTRACT_MAP


class HistoricalDataResult(enum.Enum):
    NONE = 1
    OK = 2
    EXISTS = 3
    EXCEED = 4
    LOW = 5


MONTH_LIST = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
BARCHART_URL = 'https://www.barchart.com/'


def month_from_contract_letter(contract_letter):
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


def create_bc_session(config_obj: dict, do_login=True):

    # start a session
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    if do_login is True and \
            "barchart_username" not in config_obj or \
            "barchart_password" not in config_obj:
        raise Exception('Barchart credentials are required')

    if do_login:

        # GET the login page, scrape to get CSRF token
        resp = session.get(BARCHART_URL + 'login')
        soup = BeautifulSoup(resp.text, 'html.parser')
        tag = soup.find(type='hidden')
        csrf_token = tag.attrs['value']
        logging.info(f"GET {BARCHART_URL + 'login'}, status: {resp.status_code}, CSRF token: {csrf_token}")

        # login to site
        payload = {
            'email': config_obj['barchart_username'],
            'password': config_obj['barchart_password'],
            '_token': csrf_token
        }
        resp = session.post(BARCHART_URL + 'login', data=payload)
        logging.info(f"POST {BARCHART_URL + 'login'}, status: {resp.status_code}")
        if resp.url == BARCHART_URL + 'login':
            raise Exception('Invalid Barchart credentials')

    return session


def save_prices_for_contract(
        contract,
        session,
        path,
        inv_map,
        period='hourly',
        tick_date=None,
        days=120,
        dry_run=False):

    now = datetime.now()
    low_data = False

    try:
        year_code = int(contract[len(contract)-2:])
        month_code = contract[len(contract)-3]
        if year_code > 30:
            year = 1900 + year_code
        else:
            year = 2000 + year_code
        month = month_from_contract_letter(month_code.upper())
        market_code = contract[:len(contract)-3]
        instrument = inv_map[market_code.upper()]
        datecode = str(year)+'{0:02d}'.format(month)

        filename = f"{instrument}_{datecode}00.csv"
        full_path = f"{path}/{filename}"

        # do we have this file already?
        if os.path.isfile(full_path):
            if file_is_placeholder_for_no_hourly_data(full_path):
                logging.info("Placeholder found indicating missing hourly data, switching to daily")
                period = 'daily'
            else:
                logging.info(f"Data for contract '{contract}' already downloaded, skipping\n")
                return HistoricalDataResult.EXISTS

        # we need to work out a date range for which we want the prices

        # for expired contracts the end date would be the expiry date;
        # for KISS sake, lets assume expiry is last date of contract month
        end_date = datetime(year, month, calendar.monthrange(year, month)[1])

        # but, if that end_date is in the future, then we may as well make it today...
        if now.date() < end_date.date():
            end_date = now

        # assumption no.2: lets set start date at <day_count> days before end date
        day_count = timedelta(days=days)
        start_date = end_date - day_count

        # hourly data only goes back to a certain date, depending on the exchange
        # if our dates are before that date, switch to daily prices
        if tick_date is not None and start_date < tick_date:
            logging.info(f"Switching to daily prices for '{contract}', "
                         f"{start_date.strftime('%Y-%m-%d')} is before "
                         f"{tick_date.strftime('%Y-%m-%d')}")
            period = 'daily'

    # catch/rethrow KeyError FX

    except Exception as e:  # skipcq broad by design
        logging.error(f"Problem: {e}, {traceback.format_exc()}")

    logging.info(f"getting historic {period} prices for contract '{contract}', "
                 f"from {start_date.strftime('%Y-%m-%d')} "
                 f"to {end_date.strftime('%Y-%m-%d')}")

    try:

        # open historic data download page for required contract
        url = f"{BARCHART_URL}futures/quotes/{contract}/historical-download"
        hist_resp = session.get(url)
        logging.info(f"GET {url}, status {hist_resp.status_code}")

        if hist_resp.status_code != 200:
            logging.info(f"No downloadable data found for contract '{contract}'\n")
            return HistoricalDataResult.NONE

        xsrf = urllib.parse.unquote(hist_resp.cookies['XSRF-TOKEN'])

        # scrape page for csrf_token
        hist_soup = BeautifulSoup(hist_resp.text, 'html.parser')
        hist_tag = hist_soup.find(name='meta', attrs={'name': 'csrf-token'})
        hist_csrf_token = hist_tag.attrs['content']

        # check allowance
        payload = {'onlyCheckPermissions': 'true'}
        headers = {
            'content-type': 'application/x-www-form-urlencoded',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': url,
            'x-xsrf-token': xsrf
        }
        resp = session.post(BARCHART_URL + 'my/download', headers=headers, data=payload)

        allowance = json.loads(resp.text)

        if allowance.get('error') is not None:
            return HistoricalDataResult.EXCEED

        if allowance['success']:

            logging.info(f"POST {BARCHART_URL + 'my/download'}, "
                         f"status: {resp.status_code}, "
                         f"allowance success: {allowance['success']}, "
                         f"allowance count: {allowance['count']}")

            # download data
            xsrf = urllib.parse.unquote(resp.cookies['XSRF-TOKEN'])
            headers = {
                'content-type': 'application/x-www-form-urlencoded',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': url,
                'x-xsrf-token': xsrf
            }

            payload = {'_token': hist_csrf_token,
                       'fileName': contract + '_Daily_Historical Data',
                       'symbol': contract,
                       'fields': 'tradeTime.format(Y-m-d),openPrice,highPrice,lowPrice,lastPrice,volume',
                       'startDate': start_date.strftime("%Y-%m-%d"),
                       'endDate': end_date.strftime("%Y-%m-%d"),
                       'orderBy': 'tradeTime',
                       'orderDir': 'asc',
                       'method': 'historical',
                       'limit': '10000',
                       'customView': 'true',
                       'pageTitle': 'Historical Data'}

            if period == 'daily':
                payload['type'] = 'eod'
                payload['period'] = 'daily'
                dateformat = '%Y-%m-%d'

            if period == 'hourly':
                payload['type'] = 'minutes'
                payload['interval'] = 60
                dateformat = '%m/%d/%Y %H:%M'

            if not dry_run:
                resp = session.post(BARCHART_URL + 'my/download', headers=headers, data=payload)
                logging.info(f"POST {BARCHART_URL + 'my/download'}, "
                             f"status: {resp.status_code}, "
                             f"data length: {len(resp.content)}")
                if resp.status_code == 200:

                    if 'Error retrieving data' not in resp.text:

                        iostr = io.StringIO(resp.text)
                        df = pd.read_csv(iostr, skipfooter=1, engine='python')
                        df['Time'] = pd.to_datetime(df['Time'], format=dateformat)
                        df.set_index('Time', inplace=True)
                        df.index = df.index.tz_localize(tz='US/Central').tz_convert('UTC')
                        df = df.rename(columns={"Last": "Close"})

                        if len(df) < 3:
                            low_data = True

                        filename = f"{instrument}_{datecode}00.csv"
                        full_path = f"{path}/{filename}"
                        logging.info(f"writing to: {full_path}")

                        df.to_csv(full_path, date_format='%Y-%m-%dT%H:%M:%S%z')

                    else:
                        logging.info(f"Barchart data problem for '{instrument}_{datecode}00', not writing")

            else:
                logging.info(f"Not POSTing to {BARCHART_URL + 'my/download'}, dry_run")

            logging.info(f"Finished getting Barchart historic prices for {contract}\n")

        return HistoricalDataResult.LOW if low_data else HistoricalDataResult.OK

    except Exception as e:  # skipcq broad by design
        logging.error(f"Error {e}")


def get_barchart_downloads(
        session,
        contract_map=None,
        save_directory=None,
        start_year=1950,
        end_year=2025,
        dry_run=False,
        force_daily=False):

    low_data_contracts = []

    if contract_map is None:
        contract_map = CONTRACT_MAP

    inv_contract_map = build_inverse_map(contract_map)

    try:

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        contract_list = build_contract_list(start_year, end_year, contract_map=contract_map)

        if save_directory is None:
            download_dir = os.getcwd()
        else:
            download_dir = save_directory

        for contract in contract_list:

            # calculate earliest date for which we have hourly data
            instr = inv_contract_map[contract[:-3]]
            instr_config = contract_map[instr]
            if force_daily is True:
                tick_date = datetime.now()
            elif 'tick_date' in instr_config:
                tick_date = datetime.strptime(instr_config['tick_date'], '%Y-%m-%d')
                # we want to push this date slightly into the future to try and resolve issues around
                # the switchover date
                tick_date = tick_date + timedelta(days=90)
            else:
                tick_date = None

            if 'days_count' in instr_config:
                days_count = instr_config['days_count']
            else:
                days_count = 120

            result = save_prices_for_contract(contract, session, download_dir, inv_contract_map,
                                              tick_date=tick_date, days=days_count, dry_run=dry_run)
            if result == HistoricalDataResult.EXISTS:
                continue
            if result == HistoricalDataResult.NONE:
                continue
            if result == HistoricalDataResult.LOW:
                low_data_contracts.append(contract)
                continue
            if result == HistoricalDataResult.EXCEED:
                logging.info('Max daily download reached, aborting')
                break
            # cursory attempt to not appear like a bot
            time.sleep(0 if dry_run else randint(7, 15))

        # logout
        resp = session.get(BARCHART_URL + 'logout', timeout=10)
        logging.info(f"GET {BARCHART_URL + 'logout'}, status: {resp.status_code}")

        if low_data_contracts:
            logging.warning(f"Low/poor data found for: {low_data_contracts}, maybe check config")

    except Exception as e:  # skipcq broad by design
        logging.error(f"Error {e}")


def build_contract_list(start_year, end_year, contract_map=None):

    contracts_per_instrument = {}
    contract_list = []
    count = 0

    if contract_map is None:
        contract_map = CONTRACT_MAP

    for instr in contract_map.keys():
        config_obj = contract_map[instr]
        futures_code = config_obj['code']
        if futures_code == 'none':
            continue
        rollcycle = config_obj['cycle']
        instrument_list = []

        for year in range(start_year, end_year):
            for month_code in list(rollcycle):
                instrument_list.append(f"{futures_code}{month_code}{str(year)[len(str(year))-2:]}")
        contracts_per_instrument[instr] = instrument_list
        count = count + len(instrument_list)

    logging.info(f'Count: {count}')

    pool = cycle(contract_map.keys())

    while len(contract_list) < count:
        try:
            instr = next(pool)
        except StopIteration:
            continue
        if instr not in contracts_per_instrument:
            continue
        instr_list = contracts_per_instrument[instr]
        config_obj = contract_map[instr]
        rollcycle = config_obj['cycle']
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

    logging.info(f"Contract list: {contract_list}")
    return contract_list


def file_is_placeholder_for_no_hourly_data(path):
    size = os.path.getsize(path)
    if size < 150:
        df = pd.read_csv(path)
        df['Time'] = pd.to_datetime(df['Time'], format='%Y-%m-%dT%H:%M:%S%z')
        df.set_index('Time', inplace=True)
        if len(df) == 2 and check_row_date(df.index[-1]) and check_row_date(df.index[-2]):
            return True

    return False


def check_row_date(row_date):
    return row_date.year == 1970 and row_date.month == 1 and row_date.day == 1


def build_inverse_map(contract_map):
    return {v['code']: k for k, v in contract_map.items()}


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    config = {
        'barchart_username': 'BARCHART_USERNAME',
        'barchart_password': 'BARCHART_PASSWORD'
    }
    bc_config = {k: os.environ.get(v) for k, v in config.items() if v in os.environ}

    get_barchart_downloads(
        create_bc_session(config_obj=bc_config),
        contract_map={"AUD": {"code": "A6", "cycle": "HMUZ", "tick_date": "2009-11-24"}},
        save_directory="/home/user/barchart_data",
        start_year=2020,
        end_year=2022,
        dry_run=False)
