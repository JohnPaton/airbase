from __future__ import annotations

import json

import pytest

from airbase.download_api.api_client import CityDict, CountryDict, PropertyDict
from airbase.summary import DB
from tests import resources


@pytest.fixture
def country_dump() -> list[CountryDict]:
    with DB.cursor() as cur:
        cur.execute("SELECT country_code FROM countries;")
        return [dict(countryCode=country_code) for (country_code,) in cur]


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


@pytest.fixture
def property_dump() -> list[PropertyDict]:
    with DB.cursor() as cur:
        cur.execute("SELECT pollutant, definition_url FROM property;")
        return [
            dict(notation=pollutant, id=definition_url)
            for (pollutant, definition_url) in cur
        ]


@pytest.fixture
def db_country() -> list[dict[str, str]]:
    with DB.cursor() as cur:
        cur.execute("SELECT country_code FROM countries;")
        return [dict(ct=ct, pl=pl, shortpl=shortpl) for ct, pl, shortpl in cur]


def test_county(country_dump: list[CountryDict]):
    assert country_dump == json.loads(resources.JSON_COUNTRY_RESPONSE)


def test_city(city_dump: list[CityDict]):
    assert city_dump == json.loads(resources.JSON_CITY_RESPONSE)


def test_property(property_dump: list[PropertyDict]):
    assert property_dump == json.loads(resources.JSON_PROPERTY_RESPONSE)
