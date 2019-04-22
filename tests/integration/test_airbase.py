import airbase
import os
import pytest


@pytest.fixture(scope="module")
def client():
    """Return an initialized AirbaseClient"""
    return airbase.AirbaseClient(connect=True)


@pytest.mark.withoutresponses
def test_client_connects(client):
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.search_pollutant("O3") is not None


@pytest.mark.withoutresponses
def test_download_to_directory(client, tmpdir):
    r = client.request(country=["AD", "BE"], pl="CO", year_from="2017", year_to="2017")
    r.download_to_directory(dir=str(tmpdir), skip_existing=True)
    assert os.listdir(str(tmpdir)) != []


@pytest.mark.withoutresponses
def test_download_to_file(client, tmpdir):
    r = client.request(country="MT", pl=["NO", "NO2"], year_from="2014", year_to="2014")
    r.download_to_file(str(tmpdir / "raw.csv"))
    assert os.path.exists(str(tmpdir / "raw.csv"))


@pytest.mark.withoutresponses
def test_download_metadata(client, tmpdir):
    client.download_metadata(str(tmpdir / "metadata.csv"))
    assert os.path.exists(str(tmpdir / "metadata.csv"))
