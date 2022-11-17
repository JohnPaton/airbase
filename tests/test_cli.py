import pytest

from airbase.cli import Country, Pollutant


@pytest.mark.parametrize("country", Country)
def test_country(country: Country):
    assert country.name == country.value == str(country)


@pytest.mark.parametrize("pollutant", Pollutant)
def test_pollutant(pollutant: Pollutant):
    assert pollutant.name == pollutant.value == str(pollutant)
