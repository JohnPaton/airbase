from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from airbase.csv_api import (
    Client,
    CSVData,
    Source,
    request_info_by_city,
    request_info_by_country,
)
from airbase.summary import DB


@pytest.fixture
def client(mock_csv_api) -> Client:
    """Client with loaded mocks"""
    return Client()


@pytest.fixture(scope="module")
def pollutant_ids() -> set[int]:
    ids: set[int] = set()
    ids.update(*DB.pollutants().values())
    return ids


def test_Dataset():
    assert Source.Verified == "E1a"
    assert Source.Unverified == "E2a"
    assert Source.ALL == "ALL"
    assert (
        json.dumps(list(Source))
        == json.dumps(tuple(Source))
        == '["E1a", "E2a", "ALL"]'
    )


@pytest.mark.parametrize(
    "city,country,pollutant_ids",
    (
        pytest.param("Reykjavik", "IS", {5: "PM10", 38: "NO"}, id="pollutant"),
        pytest.param("Oslo", "NO", {}, id="no-pollutant"),
    ),
)
def test_request_info_by_city(
    city: str,
    country: str,
    pollutant_ids: dict[int, str],
    year: int = 2024,
    source: Source = Source.Unverified,
):
    if not pollutant_ids:
        assert (
            request_info_by_city(source, year, city)
            == request_info_by_city(source, year, city, pollutants=set())
            == {CSVData(country, "", source, year, city=city)}
        )
    else:
        assert request_info_by_city(
            source, year, city, pollutants=set(pollutant_ids.values())
        ) == {
            CSVData(country, id, source, year, city=city)
            for id in pollutant_ids
        }


def test_request_info_by_city_warning(
    city: str = "Bad City Name",
    year: int = 2024,
    source: Source = Source.Unverified,
):
    with pytest.warns(UserWarning, match=rf"Unknown city='{city}'"):
        assert not request_info_by_city(source, year, city)


@pytest.mark.parametrize(
    "country,pollutant_ids",
    (
        pytest.param("IS", {5: "PM10", 38: "NO"}, id="pollutant"),
        pytest.param("NO", {}, id="no-pollutant"),
    ),
)
def test_request_info_by_country(
    country: str,
    pollutant_ids: dict[int, str],
    year: int = 2024,
    source: Source = Source.Unverified,
):
    if not pollutant_ids:
        assert (
            request_info_by_country(source, year, country)
            == request_info_by_country(source, year, country, pollutants=set())
            == {CSVData(country, "", source, year)}
        )
    else:
        assert request_info_by_country(
            source, year, country, pollutants=set(pollutant_ids.values())
        ) == {CSVData(country, id, source, year) for id in pollutant_ids}


def test_request_info_by_country_warning(
    country: str = "Bad City Name",
    year: int = 2024,
    source: Source = Source.Unverified,
):
    with pytest.warns(UserWarning, match=rf"Unknown country='{country}'"):
        assert not request_info_by_country(source, year, country)


@pytest.mark.parametrize(
    "info,param",
    (
        pytest.param(
            CSVData("NO", 5, Source.ALL, 2024),
            '{"CountryCode": "NO", "Pollutant": 5, "Year_from": 2024, "Year_to": 2024, "Source": "ALL", "Output": "TEXT"}',
            id="NO-ALL-PM10",
        ),
        pytest.param(
            CSVData("IS", 7, Source.Unverified, 2024, "Reykjavik"),
            '{"CountryCode": "IS", "Pollutant": 7, "Year_from": 2024, "Year_to": 2024, "Source": "E2a", "Output": "TEXT", "CityName": "Reykjavik"}',
            id="IS-E2a-O3",
        ),
    ),
)
def test_CSVData_param(info: CSVData, param: str):
    assert json.dumps(info.param()) == param, "unexpected payload"


@pytest.mark.asyncio
async def test_Client_download_urls(client: Client):
    info = CSVData("MT", 1, Source.Unverified, 2024)
    async with client:
        text = await client.download_urls(info.param())

    urls = text.strip().splitlines()
    assert len(urls) == 5

    regex = re.compile(rf"https://.*/{info.country}/.*\.csv")
    for url in urls:
        assert regex.match(url) is not None, f"wrong {url=} start"


@pytest.mark.asyncio
async def test_Client_download_binary(tmp_path: Path, client: Client):
    urls = {
        "https://data_is_here.eu/FI/data.csv": tmp_path / "FI.csv",
        "https://data_is_here.eu/NO/data.csv": tmp_path / "NO.csv",
        "https://data_is_here.eu/SE/data.csv": tmp_path / "SE.csv",
        "https://data_is_here.eu/MT/data.csv": tmp_path / "MT.csv",
    }
    assert not tuple(tmp_path.glob("*.csv"))
    async with client:
        for url, path in urls.items():
            assert await client.download_binary(url, path) == path
            assert path.is_file()

    assert len(tuple(tmp_path.glob("*.csv"))) == len(urls)
