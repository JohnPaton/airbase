from __future__ import annotations

from itertools import chain

import pytest

from airbase.summary.db import DB


def test_countries():
    countries = DB.countries()
    assert isinstance(countries, list)
    assert countries
    assert all(isinstance(country, str) for country in countries)
    assert {"NO", "DK", "SE", "DE", "IT", "FR", "NL", "GB"} <= set(countries)


POLLUTANT_IDs = {
    "PM10": {5},
    "O3": {7},
    "NO2": {8},
    "PM2.5": {6001},
    "BaP": {29, 6015, 7029},
    "o,p'-DDD": {741, 744},
}


def test_pollutants():
    pollutants = DB.pollutants()
    assert isinstance(pollutants, dict)
    assert pollutants
    assert all(isinstance(ids, set) for ids in pollutants.values())
    assert all(
        isinstance(id, int) for id in chain.from_iterable(pollutants.values())
    )
    for poll, ids in POLLUTANT_IDs.items():
        assert pollutants.get(poll) == ids


@pytest.mark.parametrize(
    "poll,ids",
    (pytest.param(poll, ids, id=poll) for poll, ids in POLLUTANT_IDs.items()),
)
def test_properties(poll: str, ids: set[str]):
    assert DB.properties(poll) == list(
        map(
            "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/{}".format,
            sorted(ids),
        )
    )


CITY_COUNTRY = {
    "Tromsø": "NO",
    "Reykjavik": "IS",
    "Göteborg": "SE",
    "Frankfurt am Main": "DE",
    "": None,
    "...": None,
}


@pytest.mark.parametrize(
    "city,country",
    (
        pytest.param(city, country, id=city)
        for city, country in CITY_COUNTRY.items()
    ),
)
def test_search_city(city: str, country: str | None):
    assert DB.search_city(city) == country
