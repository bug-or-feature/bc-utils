import os
import pytest
from bcutils.bc_utils import get_barchart_downloads, create_bc_session


@pytest.fixture(autouse=True)
def download_dir(tmp_path):
    download_dir = tmp_path / "prices"
    download_dir.mkdir()
    return download_dir.absolute()


class TestDownloader:

    def test_no_credentials(self, download_dir):
        with pytest.raises(Exception):
            get_barchart_downloads(
                create_bc_session(config_obj={}),
                contract_map={"AUD": {"code": "A6", "cycle": "HMUZ", "tick_date": "2009-11-24"}},
                save_directory=download_dir,
                start_year=2020,
                end_year=2022,
                dry_run=False)

    def test_bad_credentials(self, download_dir):
        with pytest.raises(Exception):
            get_barchart_downloads(
                create_bc_session(config_obj=dict(
                    barchart_username="user@domain.com",
                    barchart_password="s3cr3t_321")
                ),
                contract_map={"AUD": {"code": "A6", "cycle": "HMUZ", "tick_date": "2009-11-24"}},
                save_directory=download_dir,
                start_year=2020,
                end_year=2022,
                dry_run=False)

    def test_good_credentials(self, download_dir):

        config = {
            'barchart_username': 'BARCHART_USERNAME',
            'barchart_password': 'BARCHART_PASSWORD'
        }
        bc_config = {k: os.environ.get(v) for k, v in config.items() if v in os.environ}

        if len(bc_config) == 0:
            pytest.skip('Skipping good_credentials test, no Barchart credentials found in env')
        else:
            get_barchart_downloads(
                create_bc_session(config_obj=bc_config),
                contract_map={"AUD": {"code": "A6", "cycle": "H", "tick_date": "2009-11-24"}},
                save_directory=download_dir,
                start_year=2020,
                end_year=2021,
                dry_run=False)

            csv = download_dir / "AUD_20200300.csv"
            assert csv.exists()
            assert not csv.is_dir()
