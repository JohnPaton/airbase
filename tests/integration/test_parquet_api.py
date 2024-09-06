from __future__ import annotations

import re
from pathlib import Path

import pytest
import pytest_asyncio

from airbase.parquet_api import (
    Dataset,
    ParquetData,
    Session,
)
from airbase.summary import COUNTRY_CODES, DB
from tests import resources


@pytest.fixture(scope="module")
def session() -> Session:
    return Session()


@pytest_asyncio.fixture
async def countries(session: Session) -> list[str]:
    async with session:
        return list(await session.countries)


@pytest_asyncio.fixture
async def pollutants(session: Session) -> dict[str, set[int]]:
    async with session:
        return dict(await session.pollutants)


@pytest_asyncio.fixture
async def country_cities(session: Session, country: str) -> dict[str, set[str]]:
    async with session:
        return dict(await session.cities(country))


@pytest.mark.asyncio
async def test_countries(countries: list[str]):
    assert set(countries) == COUNTRY_CODES


@pytest.mark.asyncio
async def test_pollutants(pollutants: dict[str, set[str]]):
    assert pollutants == DB.pollutants()


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
async def test_cities_invalid_country(session: Session):
    async with session:
        with pytest.warns(UserWarning, match="Unknown country"):
            cities = await session.cities("Norway", "Finland", "USA")

    assert not cities, "dict is not empty"


@pytest.mark.parametrize(
    "pollutant,country,files,size",
    (
        pytest.param("NO2", "DE", 1027, 1042, id="NO2-DE"),
        pytest.param("O3", "DE", 547, 778, id="O3-DE"),
        pytest.param("NO2", "NL", 132, 144, id="NO2-NL"),
        pytest.param("O3", "NL", 93, 129, id="O3-NL"),
    ),
)
@pytest.mark.asyncio
async def test_summary(
    session: Session,
    pollutant: str,
    country: str,
    files: int,
    size: int,
):
    async with session:
        summary = await session.summary(
            ParquetData(country, Dataset.Historical, frozenset({pollutant}))
        )
    assert summary == dict(numberFiles=files, size=size)


@pytest.mark.asyncio
async def test_url_to_files(session: Session):
    info = ParquetData("MT", Dataset.Historical, city="Valletta")
    async with session:
        async for urls in session.url_to_files(info):
            pass

    assert urls
    regex = re.compile(r"https://.*/MT/.*\.parquet")
    for url in urls:
        assert regex.match(url) is not None, f"wrong {url=} start"
    assert len(urls) == 22


@pytest.mark.asyncio
async def test_download_to_directory(session: Session, tmp_path: Path):
    assert not tuple(tmp_path.glob("??/*.parquet"))
    urls = tuple(resources.CSV_PARQUET_URLS_RESPONSE.splitlines())[-5:]
    async with session:
        await session.download_to_directory(tmp_path, *urls)
    assert len(tuple(tmp_path.glob("??/*.parquet"))) == len(urls) == 5
