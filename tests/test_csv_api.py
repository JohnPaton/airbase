from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

import pytest

from airbase.csv_api import (
    Client,
    CSVData,
    Session,
    Source,
    download,
    request_info_by_city,
    request_info_by_country,
)
from tests import resources


@pytest.fixture
def client(mock_csv_api) -> Client:
    """Client with loaded mocks"""
    return Client()


@pytest.fixture
def session(mock_csv_api) -> Session:
    """Session with loaded mocks"""
    return Session()


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


@pytest.mark.asyncio
async def test_Session_url_to_files(session: Session):
    info = CSVData("MT", 1, Source.Unverified, 2024)
    assert session.number_of_urls == 0
    async with session:
        await session.url_to_files(info)

        count = Counter(session.urls)
        assert len(count) == session.number_of_urls == 5
        assert set(count.values()) == {1}, "repeated URLs"

        regex = re.compile(rf"https://.*/{info.country}/.*\.csv")
        for url in session.urls:
            assert regex.match(url) is not None, f"wrong {url=} start"

    assert session.number_of_urls == 0


@pytest.mark.parametrize(
    "country_subdir,pattern",
    (
        pytest.param(True, "??/*.csv", id="subdir"),
        pytest.param(False, "*.csv", id="flat"),
    ),
)
@pytest.mark.asyncio
async def test_Session_download_to_directory(
    tmp_path: Path, session: Session, country_subdir: bool, pattern: str
):
    assert session.number_of_urls == 0
    session.add_urls(resources.LEGACY_CSV_URLS_RESPONSE.strip().splitlines())
    assert session.number_of_urls == 5

    assert not tuple(tmp_path.rglob("*.csv"))
    async with session:
        await session.download_to_directory(
            tmp_path, country_subdir=country_subdir
        )
        assert session.number_of_urls == 0

    assert len(tuple(tmp_path.glob(pattern))) == 5


@pytest.mark.asyncio
async def test_Session_download_to_directory_warning(
    tmp_path: Path, session: Session
):
    assert session.number_of_urls == 0
    assert not tuple(tmp_path.rglob("*.csv"))
    async with session:
        with pytest.warns(UserWarning, match="No URLs to download"):
            await session.download_to_directory(tmp_path)
        assert session.number_of_urls == 0

    assert not tuple(tmp_path.rglob("*.csv"))


@pytest.mark.asyncio
async def test_Session_download_metadata(tmp_path: Path, session: Session):
    path = tmp_path / "metadata.tsv"
    assert not path.is_file()
    async with session:
        await session.download_metadata(path)

    assert path.is_file(), "missing metadata"
    assert path.stat().st_size > 0, "empty metadata"

    with path.open() as file:
        assert len(file.readlines()) == 58_687


@pytest.mark.asyncio
async def test_download(tmp_path: Path, session: Session):
    assert session.number_of_urls == 0
    assert not tuple(tmp_path.rglob("*.csv"))
    assert not tuple(tmp_path.rglob("*.tsv"))

    await download(
        Source.Unverified,
        2024,
        tmp_path,
        countries={"MT"},
        cities={"Valletta"},
        metadata=True,
        session=session,
    )

    assert session.number_of_urls == 0
    assert len(tuple(tmp_path.glob("MT/*.csv"))) == 5

    metadata = tmp_path.joinpath("metadata.tsv")
    assert metadata.is_file(), "missing metadata"
    assert metadata.stat().st_size > 0, "empty metadata"
