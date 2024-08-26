from itertools import chain
from pathlib import Path

import pytest

from airbase.download_api import (
    COUNTRY_CODES,
    cities,
    countries,
    download,
    pollutants,
)


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


def test_cities():
    known_cities = dict(
        IS={"Reykjavik"},
        NO={
            "Bergen",
            "Kristiansand",
            "Oslo",
            "Stavanger",
            "Tromsø",
            "Trondheim",
        },
        SE={
            "Borås",
            "Göteborg",
            "Helsingborg",
            "Jönköping",
            "Linköping",
            "Lund",
            "Malmö",
            "Norrköping",
            "Örebro",
            "Sodertalje",
            "Stockholm (greater city)",
            "Umeå",
            "Uppsala",
            "Västerås",
        },
        FI={
            "Helsinki / Helsingfors (greater city)",
            "Jyväskylä",
            "Kuopio",
            "Lahti / Lahtis",
            "Oulu",
            "Tampere / Tammerfors",
            "Turku / Åbo",
        },
    )
    for country, cities_ in cities(*known_cities).items():
        assert cities_ <= known_cities[country], f"missing cities on {country}"


def test_cities_invalid_country():
    countries = ("Norway", "Finland", "USA")
    with pytest.warns(UserWarning, match="Unknown country"):
        assert not cities(*countries), "dict is not empty"


def test_download(tmp_path):
    download(
        destination=tmp_path,
        pollutants="O3",
        countries="NL",
        cities="Greater Amsterdam",
    )

    files = list(Path(tmp_path).glob("*"))
    assert len(files) > 0
    for file in files:
        assert file.suffix == ".parquet"
