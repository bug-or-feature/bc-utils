import pytest

from bcutils.bc_utils import (
    Resolution,
    CONTRACT_MAP,
    create_bc_session,
    _build_save_path,
    _env,
    _get_resolution,
    _filename_from_barchart_id,
    _build_inverse_map,
    _insufficient_data,
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
