from __future__ import annotations

import re
from itertools import chain
from pathlib import Path

import pytest
import pytest_asyncio

from airbase.download_api import (
    COUNTRY_CODES,
    POLLUTANT_NOTATIONS,
    Dataset,
    DownloadInfo,
    DownloadSession,
)
from tests import resources


@pytest.fixture(scope="module")
def session() -> DownloadSession:
    return DownloadSession()


@pytest_asyncio.fixture
async def countries(session: DownloadSession) -> list[str]:
    async with session:
        return list(await session.countries())


@pytest_asyncio.fixture
async def pollutants(session: DownloadSession) -> dict[str, set[int]]:
    async with session:
        return dict(await session.pollutants())


@pytest_asyncio.fixture
async def country_cities(
    session: DownloadSession, country: str
) -> dict[str, set[str]]:
    async with session:
        return dict(await session.cities(country))


@pytest.mark.asyncio
async def test_COUNTRY_CODES(countries: list[str]):
    assert COUNTRY_CODES == set(countries)


@pytest.mark.asyncio
async def test_POLLUTANT_NOTATIONS(pollutants: dict[str, set[str]]):
    assert POLLUTANT_NOTATIONS == pollutants.keys()


@pytest.mark.asyncio
async def test_pollutants(pollutants: dict[str, set[str]]):
    assert len(pollutants) >= 469, "too few pollutants"

    ids = tuple(chain.from_iterable(pollutants.values()))
    assert len(ids) == len(set(ids)) >= 648, "too few IDs"

    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "SO2": 1}.items():
        assert pollutants.get(poll) == {id}, f"unknown {poll} {id=}"


@pytest.mark.parametrize(
    "country,cities",
    (
        pytest.param("IS", {"Reykjavik"}, id="IS"),
        pytest.param(
            "NO",
            {
                "Bergen", "Kristiansand", "Oslo", "Stavanger", "Tromsø", "Trondheim",
            },
            id="NO",
        ),
        pytest.param(
            "SE",
            {
                "Borås", "Göteborg", "Helsingborg", "Jönköping", "Linköping", "Lund",
                "Malmö", "Norrköping", "Örebro", "Sodertalje", "Stockholm (greater city)",
                "Umeå", "Uppsala", "Västerås",
            },
            id="SE",
        ),
    ),
)  # fmt: skip
@pytest.mark.asyncio
async def test_cities(
    country_cities: dict[str, set[str]], country: str, cities: set[str]
):
    assert country_cities == {country: cities}


@pytest.mark.asyncio
async def test_cities_invalid_country(session: DownloadSession):
    async with session:
        with pytest.warns(UserWarning, match="Unknown country"):
            cities = await session.cities("Norway", "Finland", "USA")

    assert not cities, "dict is not empty"


@pytest.mark.asyncio
async def test_url_to_files(session: DownloadSession):
    async with session:
        urls = await session.url_to_files(
            DownloadInfo(None, "MT", Dataset.Historical, "Valletta")
        )
    regex = re.compile(r"https://.*/MT/.*\.parquet")
    for url in urls:
        assert regex.match(url) is not None, f"wrong {url=} start"
    assert len(urls) == 22


@pytest.mark.asyncio
async def test_download_to_directory(session: DownloadSession, tmp_path: Path):
    assert not tuple(tmp_path.glob("??/*.parquet"))
    urls = tuple(resources.CSV_PARQUET_URLS_RESPONSE.splitlines())[-5:]
    async with session:
        await session.download_to_directory(
            tmp_path, *urls, raise_for_status=True
        )
    assert len(tuple(tmp_path.glob("??/*.parquet"))) == len(urls) == 5
