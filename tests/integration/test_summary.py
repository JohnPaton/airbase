from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from airbase.download_api import COUNTRY_CODES, DownloadAPI
from airbase.summary import DB

if TYPE_CHECKING:
    from airbase.download_api.api_client import (
        CityDict,
        CountryDict,
        PropertyDict,
    )


@pytest.fixture(scope="module")
def client() -> DownloadAPI:
    return DownloadAPI()


@pytest_asyncio.fixture(scope="module")
async def country_json(client: DownloadAPI) -> list[CountryDict]:
    async with client:
        return await client._get("/Country", encoding="UTF-8")


@pytest_asyncio.fixture(scope="module")
async def city_json(client: DownloadAPI) -> list[CityDict]:
    async with client:
        return await client._post(
            "/City", tuple(COUNTRY_CODES), encoding="UTF-8"
        )


@pytest_asyncio.fixture(scope="module")
async def property_json(client: DownloadAPI) -> list[PropertyDict]:
    async with client:
        return await client._get("/Property", encoding="UTF-8")


@pytest.fixture
def city_dump() -> list[CityDict]:
    with DB.cursor() as cur:
        cur.execute(
            "SELECT country_code, city_name FROM city WHERE city_name IS NOT NULL;"
        )
        return [
            dict(countryCode=country_code, cityName=city_name)
            for (country_code, city_name) in cur
        ]


def test_county(country_json: list[CountryDict]):
    assert DB.country_json() == country_json


def test_city(city_json: list[CityDict]):
    assert DB.city_json() == city_json


def test_property(property_json: list[PropertyDict]):
    assert DB.property_json() == property_json
