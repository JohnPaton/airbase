from __future__ import annotations

import json
import re
from itertools import chain
from pathlib import Path

import pytest

from airbase.download_api import (
    Dataset,
    DownloadAPI,
    DownloadInfo,
    DownloadSession,
    download,
)
from airbase.summary import COUNTRY_CODES, POLLUTANT_NAMES
from tests.resources import CSV_PARQUET_URLS_RESPONSE


@pytest.fixture
def client(mock_api) -> DownloadAPI:
    """DownloadAPI with loaded mocks"""
    return DownloadAPI()


@pytest.fixture
def session(client: DownloadAPI) -> DownloadSession:
    """DownloadSession with loaded mocks"""
    return DownloadSession(custom_client=client)


def test_Dataset():
    assert Dataset.Historical == Dataset.Airbase == 3
    assert Dataset.Verified == Dataset.E1a == 2
    assert Dataset.Unverified == Dataset.UDT == Dataset.E2a == 1
    assert (
        json.dumps(list(Dataset)) == json.dumps(tuple(Dataset)) == "[3, 2, 1]"
    )


@pytest.mark.parametrize(
    "city,country,pollutant",
    (
        pytest.param("Reykjavik", "IS", {"PM10", "NO"}, id="Reykjavik"),
        pytest.param("Oslo", "NO", set(), id="Oslo"),
        pytest.param("Göteborg", "SE", None, id="Göteborg"),
    ),
)
def test_Dataset_by_city(
    city: str,
    country: str,
    pollutant: set[str] | None,
    dataset: Dataset = Dataset.Historical,
):
    assert list(dataset.by_city(city, pollutant=pollutant)) == [
        DownloadInfo(country, dataset, pollutant, city)
    ]


def test_Dataset_by_city_warning(
    city: str = "Bad City Name",
    dataset: Dataset = Dataset.Historical,
):
    with pytest.warns(UserWarning, match=rf"Unknown city='{city}'"):
        assert not list(dataset.by_city(city))


@pytest.mark.parametrize(
    "country,pollutant",
    (
        pytest.param("IS", {"PM10", "NO"}, id="IS"),
        pytest.param("NO", set(), id="NO"),
        pytest.param("SE", None, id="SE"),
    ),
)
def test_Dataset_by_country(
    country: str,
    pollutant: set[str] | None,
    dataset: Dataset = Dataset.Historical,
):
    assert list(dataset.by_country(country, pollutant=pollutant)) == [
        DownloadInfo(country, dataset, pollutant)
    ]


def test_Dataset_by_country_warning(
    country: str = "Bad City Name",
    dataset: Dataset = Dataset.Historical,
):
    with pytest.warns(UserWarning, match=rf"Unknown country='{country}'"):
        assert not list(dataset.by_country(country))


@pytest.mark.parametrize(
    "pollutant,country,city,historical,verified,unverified",
    (
        pytest.param(
            "PM10",
            "NO",
            None,
            '{"countries": ["NO"], "cities": [], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"], "datasets": [3], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"], "datasets": [2], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"], "datasets": [1], "source": "API"}',
            id="PM10-NO",
        ),
        pytest.param(
            "O3",
            "IS",
            "Reykjavik",
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"], "datasets": [3], "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"], "datasets": [2], "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"], "datasets": [1], "source": "API"}',
            id="O3-IS",
        ),
    ),
)
def test_DownloadInfo_request_info(
    pollutant: str,
    country: str,
    city: str | None,
    historical: str,
    verified: str,
    unverified: str,
):
    assert (
        json.dumps(
            DownloadInfo(
                country, Dataset.Historical, {pollutant}, city
            ).request_info()
        )
        == historical
    ), "unexpected historical info"
    assert (
        json.dumps(
            DownloadInfo(
                country, Dataset.Verified, {pollutant}, city
            ).request_info()
        )
        == verified
    ), "unexpected verified info"
    assert (
        json.dumps(
            DownloadInfo(
                country, Dataset.Unverified, {pollutant}, city
            ).request_info()
        )
        == unverified
    ), "unexpected unverified info"


@pytest.mark.asyncio
async def test_DownloadAPI_country(client: DownloadAPI):
    async with client:
        payload = await client.country()

    assert len(payload) == len(COUNTRY_CODES)

    country_codes = set(country["countryCode"] for country in payload)
    assert country_codes == COUNTRY_CODES


@pytest.mark.asyncio
async def test_DownloadAPI_property(client: DownloadAPI):
    async with client:
        payload = await client.property()

    # some pollutants have more than one ID
    assert len(payload) >= len(POLLUTANT_NAMES)

    notations = set(pollutant["notation"] for pollutant in payload)
    assert notations == POLLUTANT_NAMES


@pytest.mark.asyncio
async def test_DownloadAPI_city(client: DownloadAPI):
    async with client:
        payload = await client.city(tuple(COUNTRY_CODES))

    # come countries have no cities
    country_codes = set(country["countryCode"] for country in payload)
    assert country_codes <= COUNTRY_CODES
    assert country_codes == COUNTRY_CODES - {
        "AD", "AL", "BA", "GI", "LI", "ME", "MK", "RS", "TR", "XK",
    }  # fmt: skip


@pytest.mark.asyncio
async def test_DownloadAPI_download_urls(client: DownloadAPI):
    info = DownloadInfo("MT", Dataset.Historical, None, "Valletta")
    async with client:
        async for urls in client.download_urls({info}, raise_for_status=True):
            pass

    assert urls
    regex = re.compile(rf"https://.*/{info.country}/.*\.parquet")
    for url in urls:
        assert regex.match(url) is not None, f"wrong {url=} start"
    assert len(urls) == 22


@pytest.mark.asyncio
async def test_DownloadAPI_download_binary_files(
    tmp_path: Path, client: DownloadAPI
):
    urls = {
        "https://data_is_here.eu/FI/data.parquet": tmp_path / "FI.parquet",
        "https://data_is_here.eu/NO/data.parquet": tmp_path / "NO.parquet",
        "https://data_is_here.eu/SE/data.parquet": tmp_path / "SE.parquet",
        "https://data_is_here.eu/MT/data.parquet": tmp_path / "MT.parquet",
    }
    assert not tuple(tmp_path.glob("*.parquet"))
    async with client:
        async for path in client.download_binary_files(
            urls, raise_for_status=True
        ):
            assert path in urls.values()

    assert len(tuple(tmp_path.glob("*.parquet"))) == len(urls)


@pytest.mark.asyncio
async def test_DownloadSession_country(session: DownloadSession):
    async with session:
        country_codes = await session.countries

    assert len(country_codes) == len(set(country_codes)) == len(COUNTRY_CODES)
    assert set(country_codes) == COUNTRY_CODES


@pytest.mark.asyncio
async def test_DownloadSession_pollutants(session: DownloadSession):
    async with session:
        pollutants = await session.pollutants

    assert pollutants.keys() == POLLUTANT_NAMES

    ids = tuple(chain.from_iterable(pollutants.values()))
    assert len(ids) == len(set(ids)) == 648


@pytest.mark.asyncio
async def test_DownloadSession_city(session: DownloadSession):
    async with session:
        cities = await session.cities()

    # come countries have no cities
    assert cities.keys() <= COUNTRY_CODES
    assert cities.keys() == COUNTRY_CODES - {
        "AD", "AL", "BA", "GI", "LI", "ME", "MK", "RS", "TR", "XK",
    }  # fmt: skip

    assert cities["IS"] == {"Reykjavik"}
    assert cities["NO"] == {
        "Bergen", "Kristiansand", "Oslo", "Stavanger", "Tromsø", "Trondheim",
    }  # fmt: skip
    assert cities["SE"] == {
        "Borås", "Göteborg", "Helsingborg", "Jönköping", "Linköping", "Lund",
        "Malmö", "Norrköping", "Örebro", "Sodertalje", "Stockholm (greater city)",
        "Umeå", "Uppsala", "Västerås",
    }  # fmt: skip


@pytest.mark.asyncio
async def test_DownloadSession_url_to_files(session: DownloadSession):
    info = DownloadInfo("MT", Dataset.Historical, None, "Valletta")
    async with session:
        urls = await session.url_to_files(info)

    assert urls
    regex = re.compile(rf"https://.*/{info.country}/.*\.parquet")
    for url in urls:
        assert regex.match(url) is not None, f"wrong {url=} start"
    assert len(urls) == 22


@pytest.mark.asyncio
async def test_DownloadSession_download_to_directory(
    tmp_path: Path, session: DownloadSession
):
    assert not tuple(tmp_path.glob("??/*.parquet"))
    urls = tuple(CSV_PARQUET_URLS_RESPONSE.splitlines())[-5:]
    async with session:
        await session.download_to_directory(
            tmp_path, *urls, raise_for_status=True
        )
    assert len(tuple(tmp_path.glob("??/*.parquet"))) == len(urls) == 5


@pytest.mark.asyncio
async def test_download(tmp_path: Path, session: DownloadSession):
    assert not tuple(tmp_path.glob("MT/*.parquet"))
    await download(
        Dataset.Historical,
        tmp_path,
        countries={"MT"},
        cities={"Valletta"},
        session=session,
    )
    assert len(tuple(tmp_path.glob("MT/*.parquet"))) == 22
