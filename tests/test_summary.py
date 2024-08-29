from itertools import chain

from airbase.summary.db import DB


def test_countries():
    countries = DB.countries()
    assert isinstance(countries, list)
    assert countries
    assert all(isinstance(country, str) for country in countries)
    for country in ["NO", "DK", "SE", "DE", "IT", "FR", "NL", "GB"]:
        assert country in countries


def test_pollutants():
    pollutants = DB.pollutants()
    assert isinstance(pollutants, dict)
    assert pollutants
    assert all(isinstance(ids, set) for ids in pollutants.values())
    assert all(
        isinstance(id, int) for id in chain.from_iterable(pollutants.values())
    )
    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "PM2.5": 6001}.items():
        assert pollutants.get(poll) == {id}


def test_properties():
    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "PM2.5": 6001}.items():
        assert DB.properties(poll) == [
            f"http://dd.eionet.europa.eu/vocabulary/aq/pollutant/{id}"
        ]
