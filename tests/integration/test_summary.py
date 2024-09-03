from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from airbase.download_api import Client
from airbase.summary import COUNTRY_CODES, DB

if TYPE_CHECKING:
    from airbase.download_api.api_types import (
        CityJSON,
        CountryJSON,
        PropertyJSON,
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
        return await client.city(tuple(COUNTRY_CODES))


@pytest_asyncio.fixture(scope="module")
async def property_json(client: Client) -> PropertyJSON:
    async with client:
        return await client.property()


def test_county(country_json: CountryJSON):
    assert DB.country_json() == country_json


def test_city(city_json: CityJSON):
    assert DB.city_json() == city_json


def test_property(property_json: PropertyJSON):
    assert DB.property_json() == property_json
