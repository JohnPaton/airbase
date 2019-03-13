import airbase
import os

import pytest


@pytest.mark.withoutresponses
@pytest.fixture(scope="module")
def client():
    """Return an initialized AirbaseClient"""
    return airbase.AirbaseClient()


@pytest.mark.withoutresponses
def test_client_connect(client, tmpdir):
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.search_pollutant("O3") is not None


@pytest.mark.withoutresponses
def test_metadata(client, tmpdir):
    client.download_metadata(tmpdir / "metadata.csv")
    assert os.path.exists(tmpdir / "metadata.csv")


@pytest.mark.withoutresponses
def test_download_to_directory(client, tmpdir):
    r = client.request(
        country=["AD", "MT"], pl="SO2", year_from="2013", year_to="2013"
    )
    r.download_to_directory(dir=tmpdir)
    assert os.listdir(tmpdir) != []


@pytest.mark.withoutresponses
def test_download_to_file(client, tmpdir):
    r = client.request(
        country="AD", pl=["CO", "NO"], year_from="2014", year_to="2014"
    )
    r.download_to_file(tmpdir / "raw.csv")
    assert os.path.exists(tmpdir / "raw.csv")
