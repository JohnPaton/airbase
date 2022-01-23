from pathlib import Path

import pytest

import airbase
from tests import resources


@pytest.fixture
def client(summary_response, metadata_response) -> airbase.AirbaseClient:
    """initialied client with mocked responses"""
    return airbase.AirbaseClient()


def test_init_connect_false(summary_response):
    client = airbase.AirbaseClient(connect=False)
    with pytest.raises(AttributeError):
        client.all_countries
    with pytest.raises(AttributeError):
        client.all_pollutants
    with pytest.raises(AttributeError):
        client.pollutants_per_country
    with pytest.raises(AttributeError):
        client.request()

    client.connect()
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.request() is not None


def test_init_connect(summary_response):
    client = airbase.AirbaseClient(connect=True)
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None


def test_download_metadata(
    client: airbase.AirbaseClient, tmp_path: Path, capsys
):
    path = tmp_path / "meta.csv"
    client.download_metadata(str(path))
    assert path.exists()
    assert path.read_text() == resources.METADATA_RESPONSE


def test_request_raises_bad_country(client: airbase.AirbaseClient):
    with pytest.raises(ValueError):
        client.request(country="lol123")

    with pytest.raises(ValueError):
        client.request(["NL", "lol123"])


@pytest.mark.parametrize("year", ["not an int", "1234", "9999"])
def test_request_raises_bad_year(client: airbase.AirbaseClient, year: str):
    with pytest.raises(ValueError):
        client.request(year_from=year)

    with pytest.raises(ValueError):
        client.request(year_to=year)


def test_request_pl(client: airbase.AirbaseClient):
    r = client.request(pl="NO")
    assert isinstance(r.shortpl, list)
    assert len(r.shortpl) == 1

    r = client.request(pl=["NO", "NO3"])
    assert isinstance(r.shortpl, list)
    assert len(r.shortpl) == 2

    with pytest.raises(ValueError):
        r = client.request(pl=["NO", "NO3", "Not a pl"])


def test_request_response_generated(client: airbase.AirbaseClient):
    r = client.request()
    assert isinstance(r, airbase.AirbaseRequest)


def test_request_not_pl_and_shortpl(client: airbase.AirbaseClient):
    with pytest.raises(ValueError):
        client.request(pl="O3", shortpl="123")


def test_search_pl_exact(client: airbase.AirbaseClient):
    result = client.search_pollutant("NO3")
    assert result[0]["pl"] == "NO3"


def test_search_pl_shortest_first(client: airbase.AirbaseClient):
    result = client.search_pollutant("N")
    names = [r["pl"] for r in result]
    assert len(names[0]) <= len(names[1])
    assert len(names[0]) <= len(names[-1])


def test_search_pl_limit(client: airbase.AirbaseClient):
    result = client.search_pollutant("N", limit=1)
    assert len(result) == 1


def test_search_pl_no_result(client: airbase.AirbaseClient):
    result = client.search_pollutant("Definitely not a pollutant")
    assert result == []


def test_search_pl_case_insensitive(client: airbase.AirbaseClient):
    result = client.search_pollutant("no3")
    assert result[0]["pl"] == "NO3"
