from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from airbase.parquet_api import Client
from airbase.summary import DB

if TYPE_CHECKING:
    from airbase.parquet_api.types import (
        CityJSON,
        CountryJSON,
        PollutantJSON,
    )


@pytest.fixture(scope="module")
def client() -> Client:
    return Client()


@pytest_asyncio.fixture(scope="module")
async def country_json(client: Client) -> CountryJSON:
    async with client:
        return await client.country()


@pytest_asyncio.fixture(scope="module")
async def city_json(client: Client) -> CityJSON:
    async with client:
        return await client.city(tuple(DB.COUNTRY_CODES))


@pytest_asyncio.fixture(scope="module")
async def pollutant_json(client: Client) -> PollutantJSON:
    async with client:
        return await client.pollutant()


def test_county(country_json: CountryJSON):
    assert DB.country_json() == country_json


def test_city(city_json: CityJSON):
    assert DB.city_json() == city_json


def test_pollutant(pollutant_json: PollutantJSON):
    assert DB.pollutant_json() == pollutant_json
