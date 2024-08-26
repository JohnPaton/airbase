from __future__ import annotations

import json
from itertools import chain
from pathlib import Path

import pytest

from airbase.download_api import (
    COUNTRY_CODES,
    Dataset,
    DownloadInfo,
    cities,
    countries,
    download_to_directory,
    pollutants,
    url_to_files,
)


def test_Dataset():
    assert Dataset.Historical == Dataset.Airbase == 3
    assert Dataset.Verified == Dataset.E1a == 2
    assert Dataset.Unverified == Dataset.UDT == Dataset.E2a == 1
    assert (
        json.dumps(list(Dataset)) == json.dumps(tuple(Dataset)) == "[3, 2, 1]"
    )


@pytest.mark.parametrize(
    "pollutant,country,cities,historical,verified,unverified",
    (
        pytest.param(
            "PM10",
            "NO",
            tuple(),
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [3], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [2], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [1], "source": "API"}',
            id="PM10-NO",
        ),
        pytest.param(
            "O3",
            "IS",
            ("Reykjavik",),
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["O3"], "datasets": [3], "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["O3"], "datasets": [2], "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["O3"], "datasets": [1], "source": "API"}',
            id="O3-IS",
        ),
    ),
)
def test_DownloadInfo(
    pollutant: str,
    country: str,
    cities: tuple[str, ...],
    historical: str,
    verified: str,
    unverified: str,
):
    assert (
        json.dumps(
            DownloadInfo.historical(pollutant, country, *cities).request_info()
        )
        == historical
    ), "unexpected historical info"
    assert (
        json.dumps(
            DownloadInfo.verified(pollutant, country, *cities).request_info()
        )
        == verified
    ), "unexpected verified info"
    assert (
        json.dumps(
            DownloadInfo.unverified(pollutant, country, *cities).request_info()
        )
        == unverified
    ), "unexpected unverified info"


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


def test_url_to_files():
    urls = url_to_files(DownloadInfo.historical("O3", "NO", "Oslo"))
    assert len(urls) == 56


def test_download_to_directory(tmp_path: Path):
    assert not tuple(tmp_path.glob("NO/*.parquet"))
    urls = (
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_501.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_503.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_504.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00008_500.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00009_500.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0011A_00020_100.parquet",
    )
    download_to_directory(tmp_path, *urls, raise_for_status=True)
    assert len(tuple(tmp_path.glob("NO/*.parquet"))) == len(urls)
