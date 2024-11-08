import pytest
from datetime import datetime

from bcutils.bc_utils import (
    Resolution,
    CONTRACT_MAP,
    create_bc_session,
    BCException,
    _build_save_path,
    _env,
    _get_resolution,
    _filename_from_barchart_id,
    _build_inverse_map,
    _insufficient_data,
    _before_available_res,
    _get_exchange_for_code,
    _instr_code_from_file_name,
    _res_from_file_name,
)

INV_MAP = _build_inverse_map(CONTRACT_MAP)


class TestUtils:
    def test_day(self):
        res = _get_resolution("/home/user/Day_GOLD_20240100.csv")
        assert res == Resolution.Day

    def test_hour(self):
        res = _get_resolution("/home/user/Hour_AUD_20240300.csv")
        assert res == Resolution.Hour

    def test_merged(self):
        with pytest.raises(Exception):
            _get_resolution("/home/user/AUD_20240300.csv")

    def test_get_day_resolution(self):
        res = _get_resolution("/home/user/data/Day_GOLD_20240100.csv")
        assert res == Resolution.Day

    def test_get_hour_resolution(self):
        res = _get_resolution("/home/user/data/Hour_AUD_20240300.csv")
        assert res == Resolution.Hour

    def test_get_bad_resolution(self):
        with pytest.raises(Exception):
            _get_resolution("/home/user/data/Min_AUD_20240300.csv")

    def test_hourly_filename(self):
        name = _filename_from_barchart_id("GCF24", INV_MAP, Resolution.Hour)
        assert name == "Hour_GOLD_20240100.csv"

    def test_daily_filename(self):
        name = _filename_from_barchart_id("A6H24", INV_MAP, Resolution.Day)
        assert name == "Day_AUD_20240300.csv"

    def test_bad_filename(self):
        with pytest.raises(Exception):
            _filename_from_barchart_id("XXA24", INV_MAP, Resolution.Day)

    def test_day_save_path(self):
        path = _build_save_path("GOLD", 1, 2024, Resolution.Day, "/home/user/data")
        assert path == "/home/user/data/Day_GOLD_20240100.csv"

    def test_hour_save_path(self):
        path = _build_save_path("AUD", 3, 2024, Resolution.Hour, "/home/user/data")
        assert path == "/home/user/data/Hour_AUD_20240300.csv"

    def test_insufficient_data(self):
        assert _insufficient_data(
            create_bc_session(config_obj=_env(), do_login=False),
            "TGF08",
            Resolution.Day,
        )

    def test_before_available_res(self):
        assert _before_available_res(
            Resolution.Day, datetime(1975, 1, 1), CONTRACT_MAP["AUD"]
        )

        assert not _before_available_res(
            Resolution.Day, datetime(1978, 3, 24), CONTRACT_MAP["AUD"]
        )

        assert _before_available_res(
            Resolution.Hour, datetime(2007, 1, 1), CONTRACT_MAP["GOLD"]
        )

        assert not _before_available_res(
            Resolution.Hour, datetime(2008, 6, 1), CONTRACT_MAP["GOLD"]
        )

        with pytest.raises(BCException):
            _before_available_res(
                Resolution.Hour,
                datetime(2007, 1, 1),
                {"code": "XX", "cycle": "HMUZ", "exchange": "BLAH"},
            )

        with pytest.raises(BCException):
            _before_available_res(
                Resolution.Hour,
                datetime(2007, 1, 1),
                {"code": "ABC", "cycle": "HMUZ"},
            )

    def test_get_exchange(self):
        exch = _get_exchange_for_code(
            create_bc_session(config_obj=_env(), do_login=False), "GCF24"
        )
        assert exch == "COMEX"

    def test_get_instr_code_from_file_name_split(self):
        instr_code = _instr_code_from_file_name("Day_EURIBOR-ICE_20240900")
        assert instr_code == "EURIBOR-ICE"

    def test_get_instr_code_from_file_name_not(self):
        instr_code = _instr_code_from_file_name("SOFR_20240900", split_freq=False)
        assert instr_code == "SOFR"

    def test_get_instr_code_from_file_name_double_not(self):
        instr_code = _instr_code_from_file_name("GAS_US_20240900", split_freq=False)
        assert instr_code == "GAS_US"

    def test_res_from_file_name_split(self):
        res = _res_from_file_name("Day_EURIBOR-ICE_20240900")
        assert res == Resolution.Day
