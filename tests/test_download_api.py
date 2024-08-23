from itertools import chain

from airbase.download_api import COUNTRY_CODES, countries, pollutants


def test_countries():
    assert set(countries()) == set(COUNTRY_CODES)


def test_pollutants():
    pollutants_ = pollutants()

    names = tuple(pollutants_)
    assert len(names) >= 469, "too few pollutants"

    ids = tuple(chain.from_iterable(pollutants_.values()))
    assert len(ids) == len(set(ids)) >= 648, "too few IDs"

    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "SO2": 1}.items():
        assert pollutants_.get(poll) == {id}, f"unknown {poll} {id=}"
