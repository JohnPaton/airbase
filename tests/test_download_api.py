from __future__ import annotations

import json
from itertools import chain
from pathlib import Path

import pytest

from airbase.download_api import (
    COUNTRY_CODES,
    Dataset,
    DownloadClient,
    DownloadInfo,
)


@pytest.fixture(scope="module")
def client():
    return DownloadClient()


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


@pytest.mark.asyncio
async def test_countries(client: DownloadClient):
    async with client:
        countries = await client.countries()

    assert set(countries) == set(COUNTRY_CODES)


@pytest.mark.asyncio
async def test_pollutants(client: DownloadClient):
    async with client:
        pollutants = await client.pollutants()

    names = tuple(pollutants)
    assert len(names) >= 469, "too few pollutants"

    ids = tuple(chain.from_iterable(pollutants.values()))
    assert len(ids) == len(set(ids)) >= 648, "too few IDs"

    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "SO2": 1}.items():
        assert pollutants.get(poll) == {id}, f"unknown {poll} {id=}"


@pytest.mark.asyncio
async def test_cities(client: DownloadClient):
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
    async with client:
        country_cities = await client.cities(*known_cities)

    for country, cities in country_cities.items():
        assert cities <= known_cities[country], f"missing cities on {country}"


@pytest.mark.asyncio
async def test_cities_invalid_country(client: DownloadClient):
    async with client:
        with pytest.warns(UserWarning, match="Unknown country"):
            cities = await client.cities("Norway", "Finland", "USA")

    assert not cities, "dict is not empty"


@pytest.mark.asyncio
async def test_url_to_files(client: DownloadClient):
    async with client:
        urls = await client.url_to_files(
            DownloadInfo.historical("O3", "NO", "Oslo")
        )
    assert len(urls) == 56


@pytest.mark.asyncio
async def test_download_to_directory(client: DownloadClient, tmp_path: Path):
    assert not tuple(tmp_path.glob("NO/*.parquet"))
    urls = (
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_501.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_503.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_504.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00008_500.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00009_500.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0011A_00020_100.parquet",
    )
    async with client:
        await client.download_to_directory(
            tmp_path, *urls, raise_for_status=True
        )
    assert len(tuple(tmp_path.glob("NO/*.parquet"))) == len(urls)
