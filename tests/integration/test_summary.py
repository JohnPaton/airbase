from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from airbase.download_api import AbstractClient, Client
from airbase.summary import COUNTRY_CODES, DB

if TYPE_CHECKING:
    from airbase.download_api.abstract_api_client import (
        CityResponse,
        CountryResponse,
        PropertyResponse,
    )


@pytest.fixture(scope="module")
def client() -> AbstractClient:
    return Client()


@pytest_asyncio.fixture(scope="module")
async def country_json(client: AbstractClient) -> CountryResponse:
    async with client:
        return await client.country()


@pytest_asyncio.fixture(scope="module")
async def city_json(client: AbstractClient) -> CityResponse:
    async with client:
        return await client.city(tuple(COUNTRY_CODES))


@pytest_asyncio.fixture(scope="module")
async def property_json(client: AbstractClient) -> PropertyResponse:
    async with client:
        return await client.property()


def test_county(country_json: CountryResponse):
    assert DB.country_json() == country_json


def test_city(city_json: CityResponse):
    assert DB.city_json() == city_json


def test_property(property_json: PropertyResponse):
    assert DB.property_json() == property_json
