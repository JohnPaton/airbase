from pathlib import Path

import pytest
from aioresponses import aioresponses

import airbase
from tests.resources import CSV_RESPONSE, METADATA_RESPONSE


@pytest.fixture(scope="module")
def client():
    """Return an initialized AirbaseClient"""
    prefixes = [
        airbase.resources.E1A_SUMMARY_URL,
        "http://fme.discomap.eea.europa.eu/",
        "https://ereporting.blob.core.windows.net/",
        airbase.resources.METADATA_URL,
    ]
    with aioresponses(passthrough=prefixes):
        yield airbase.AirbaseClient(connect=True)


def test_client_connects(client: airbase.AirbaseClient):
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.search_pollutant("O3") is not None


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
    r.download_to_file(str(path))
    assert path.exists()

    # make sure data format hasn't changed
    with open(str(tmp_path / "raw.csv")) as h:
        headers_downloaded = h.readline().strip()
    headers_expected = CSV_RESPONSE.split("\n")[0]

    assert headers_downloaded == headers_expected


def test_download_metadata(client: airbase.AirbaseClient, tmp_path: Path):
    path = tmp_path / "metadata.tsv"
    client.download_metadata(str(path))
    assert path.exists()

    # make sure metadata format hasn't changed
    headers_downloaded = path.read_text().splitlines()[0]
    headers_expected = METADATA_RESPONSE.splitlines()[0]

    assert headers_downloaded == headers_expected
