import airbase
import os
import pytest


@pytest.fixture()
def client():
    return airbase.AirbaseClient(connect=True)


def test_client_connects(client):
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.search_pollutant("O3") is not None


def test_download_to_directory(client, tmpdir):
    r = client.request(country=["AD", "BE"], pl="CO", year_from="2017", year_to="2017")
    r.download_to_directory(dir=tmpdir, skip_existing=True)
    assert os.listdir(tmpdir) != []


def test_download_to_file(client, tmpdir):
    r = client.request(country="MT", pl=["NO", "NO2"], year_from="2014", year_to="2014")
    r.download_to_file(tmpdir / "raw.csv")
    assert os.path.exists(tmpdir / "raw.csv")


def test_download_metadata(client, tmpdir):
    client.download_metadata(tmpdir / "metadata.csv")
    assert os.path.exists(tmpdir / "metadata.csv")
