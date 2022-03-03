from pathlib import Path

import pytest

import airbase
from tests.resources import CSV_RESPONSE, METADATA_RESPONSE


@pytest.fixture(scope="module")
def client():
    """Return initialized AirbaseClient instance"""
    return airbase.AirbaseClient()


def test_download_to_directory(client: airbase.AirbaseClient, tmp_path: Path):
    r = client.request(
        country=["AD", "BE"], pl="CO", year_from="2017", year_to="2017"
    )

    r.download_to_directory(dir=str(tmp_path), skip_existing=True)
    assert list(tmp_path.iterdir())


def test_download_to_file(client: airbase.AirbaseClient, tmp_path: Path):
    r = client.request(
        country="CY", pl=["As", "NO2"], year_from="2014", year_to="2014"
    )

    path = tmp_path / "raw.csv"
    r.download_to_file(path)
    assert path.exists()

    # make sure data format hasn't changed
    headers_downloaded = path.read_text().splitlines()[0]
    headers_expected = CSV_RESPONSE.splitlines()[0]

    assert headers_downloaded == headers_expected


def test_download_metadata(client: airbase.AirbaseClient, tmp_path: Path):
    path = tmp_path / "metadata.tsv"
    client.download_metadata(path)
    assert path.exists()

    # make sure metadata format hasn't changed
    headers_downloaded = path.read_text().splitlines()[0]
    headers_expected = METADATA_RESPONSE.splitlines()[0]

    assert headers_downloaded == headers_expected
