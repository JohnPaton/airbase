import os

import pytest
from aioresponses import aioresponses

import airbase


from ..resources import CSV_RESPONSE, METADATA_RESPONSE


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


@pytest.mark.withoutresponses
def test_client_connects(client: airbase.AirbaseClient):
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.search_pollutant("O3") is not None


@pytest.mark.withoutresponses
def test_download_to_directory(client: airbase.AirbaseClient, tmpdir):
    r = client.request(
        country=["AD", "BE"], pl="CO", year_from="2017", year_to="2017"
    )
    r.download_to_directory(dir=str(tmpdir), skip_existing=True)
    assert os.listdir(str(tmpdir)) != []


@pytest.mark.withoutresponses
def test_download_to_file(client: airbase.AirbaseClient, tmpdir):
    r = client.request(
        country="CY", pl=["As", "NO2"], year_from="2014", year_to="2014"
    )
    r.download_to_file(str(tmpdir / "raw.csv"))
    assert os.path.exists(str(tmpdir / "raw.csv"))

    # make sure data format hasn't changed
    with open(str(tmpdir / "raw.csv")) as h:
        headers_downloaded = h.readline().strip()
    headers_expected = CSV_RESPONSE.split("\n")[0]

    assert headers_downloaded == headers_expected


@pytest.mark.withoutresponses
def test_download_metadata(client: airbase.AirbaseClient, tmpdir):
    client.download_metadata(str(tmpdir / "metadata.tsv"))
    assert os.path.exists(str(tmpdir / "metadata.tsv"))

    # make sure metadata format hasn't changed
    with open(str(tmpdir / "metadata.tsv")) as h:
        headers_downloaded = h.readline().strip()
    headers_expected = METADATA_RESPONSE.split("\n")[0]

    assert headers_downloaded == headers_expected
