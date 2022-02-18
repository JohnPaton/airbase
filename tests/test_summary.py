import pytest

from airbase.summary import Summary
from tests.resources import SUMMARY


@pytest.fixture
def summary():
    return Summary(SUMMARY)


def test_countries(summary: Summary):
    countries = summary.countries()
    assert isinstance(countries, list)
    assert countries
    assert all(isinstance(country, str) for country in countries)


def test_pollutants(summary: Summary):
    pollutants = summary.pollutants()
    assert isinstance(pollutants, dict)
    assert pollutants
    assert all(isinstance(id, int) for id in pollutants.values())
    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "PM2.5": 6001}.items():
        assert pollutants.get(poll) == id


def test_pollutants_per_country(summary: Summary):
    output = summary.pollutants_per_country()
    assert isinstance(output, dict)
    assert output

    pollutants = output.get("AD")
    assert isinstance(pollutants, dict)
    assert all(isinstance(id, int) for id in pollutants.values())
    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "SO2": 1}.items():
        assert pollutants.get(poll) == id
