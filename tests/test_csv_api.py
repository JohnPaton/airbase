from __future__ import annotations

import json

import pytest

from airbase.csv_api import (
    CSVData,
    Source,
    request_info_by_city,
    request_info_by_country,
)


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
    "city,country,pollutant,ids",
    (
        pytest.param(
            "Reykjavik", "IS", {"PM10", "NO"}, {5, 38}, id="Reykjavik"
        ),
        pytest.param("Oslo", "NO", set(), set(), id="Oslo"),
        pytest.param("Göteborg", "SE", None, None, id="Göteborg"),
    ),
)
def test_request_info_by_city(
    city: str,
    country: str,
    pollutant: set[str] | None,
    ids: set[int] | None,
    year: int = 2024,
    source: Source = Source.Unverified,
):
    if pollutant is None:
        assert request_info_by_city(
            source, year, city, pollutant=pollutant
        ) == {CSVData(country, source, year, city=city)}
    else:
        assert ids is not None
        assert request_info_by_city(
            source, year, city, pollutant=pollutant
        ) == {CSVData(country, source, year, id, city) for id in ids}


def test_request_info_by_city_warning(
    city: str = "Bad City Name",
    year: int = 2024,
    source: Source = Source.Unverified,
):
    with pytest.warns(UserWarning, match=rf"Unknown city='{city}'"):
        assert not request_info_by_city(source, year, city)


@pytest.mark.parametrize(
    "country,pollutant,ids",
    (
        pytest.param("IS", {"PM10", "NO"}, {5, 38}, id="IS"),
        pytest.param("NO", set(), set(), id="NO"),
        pytest.param("SE", None, None, id="SE"),
    ),
)
def test_request_info_by_country(
    country: str,
    pollutant: set[str] | None,
    ids: set[int] | None,
    year: int = 2024,
    source: Source = Source.Unverified,
):
    if pollutant is None:
        assert request_info_by_country(
            source, year, country, pollutant=pollutant
        ) == {CSVData(country, source, year)}
    else:
        assert ids is not None
        assert request_info_by_country(
            source, year, country, pollutant=pollutant
        ) == {CSVData(country, source, year, id) for id in ids}


def test_request_info_by_country_warning(
    country: str = "Bad City Name",
    year: int = 2024,
    source: Source = Source.Unverified,
):
    with pytest.warns(UserWarning, match=rf"Unknown country='{country}'"):
        assert not request_info_by_country(source, year, country)


@pytest.mark.parametrize(
    "info,payload",
    (
        pytest.param(
            CSVData("NO", Source.ALL, 2024),
            '{"CountryCode": "NO", "Year_from": 2024, "Year_to": 2024, "Source": "ALL", "Output": "TEXT"}',
            id="NO-ALL",
        ),
        pytest.param(
            CSVData("IS", Source.Unverified, 2024, 7, "Reykjavik"),
            '{"CountryCode": "IS", "Year_from": 2024, "Year_to": 2024, "Source": "E2a", "Output": "TEXT", "Pollutant": 7, "CityName": "Reykjavik"}',
            id="IS-E2a-O3",
        ),
    ),
)
def test_CSVData_payload(info: CSVData, payload: str):
    assert json.dumps(info.payload()) == payload, "unexpected payload"
