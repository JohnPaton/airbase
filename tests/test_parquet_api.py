from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from airbase.parquet_api import (
    Client,
    Dataset,
    ParquetData,
    Session,
    download,
    request_info_by_city,
    request_info_by_country,
)
from airbase.summary import DB
from tests.resources import CSV_PARQUET_URLS_RESPONSE


@pytest.fixture
def client(mock_parquet_api) -> Client:
    """Client with loaded mocks"""
    return Client()


@pytest.fixture
def session(mock_parquet_api) -> Session:
    """Session with loaded mocks"""
    return Session()


def test_Dataset():
    assert Dataset.Historical == Dataset.Airbase == 3
    assert Dataset.Verified == Dataset.E1a == 2
    assert Dataset.Unverified == Dataset.UDT == Dataset.E2a == 1
    assert (
        json.dumps(list(Dataset)) == json.dumps(tuple(Dataset)) == "[3, 2, 1]"
    )


@pytest.mark.parametrize(
    "city,country,pollutants",
    (
        pytest.param("Reykjavik", "IS", {"PM10", "NO"}, id="pollutants"),
        pytest.param("Göteborg", "SE", None, id="no-pollutants"),
    ),
)
def test_request_info_by_city(
    city: str,
    country: str,
    pollutants: set[str] | None,
    dataset: Dataset = Dataset.Historical,
):
    if not pollutants:
        assert (
            request_info_by_city(dataset, city)
            == request_info_by_city(dataset, city, pollutants=set())
            == {ParquetData(country, dataset, city=city)}
        )
    else:
        assert request_info_by_city(dataset, city, pollutants=pollutants) == {
            ParquetData(country, dataset, frozenset(pollutants), city)
        }


def test_request_info_by_city_warning(
    city: str = "Bad City Name",
    dataset: Dataset = Dataset.Historical,
):
    with pytest.warns(UserWarning, match=rf"Unknown city='{city}'"):
        assert not request_info_by_city(dataset, city)


@pytest.mark.parametrize(
    "country,pollutants",
    (
        pytest.param("IS", {"PM10", "NO"}, id="IS"),
        pytest.param("NO", set(), id="NO"),
        pytest.param("SE", None, id="SE"),
    ),
)
def test_request_info_by_country(
    country: str,
    pollutants: set[str] | None,
    dataset: Dataset = Dataset.Historical,
):
    if not pollutants:
        assert (
            request_info_by_country(dataset, country)
            == request_info_by_country(dataset, country, pollutants=set())
            == {ParquetData(country, dataset)}
        )
    else:
        assert request_info_by_country(
            dataset, country, pollutants=pollutants
        ) == {ParquetData(country, dataset, frozenset(pollutants))}


def test_request_info_by_country_warning(
    country: str = "Bad City Name",
    dataset: Dataset = Dataset.Historical,
):
    with pytest.warns(UserWarning, match=rf"Unknown country='{country}'"):
        assert not request_info_by_country(dataset, country)


@pytest.mark.parametrize(
    "pollutants,country,city,historical,verified,unverified",
    (
        pytest.param(
            frozenset({"PM10"}),
            "NO",
            None,
            '{"countries": ["NO"], "cities": [], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"], "datasets": 3, "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"], "datasets": 2, "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"], "datasets": 1, "source": "API"}',
            id="PM10-NO",
        ),
        pytest.param(
            frozenset({"O3"}),
            "IS",
            "Reykjavik",
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"], "datasets": 3, "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"], "datasets": 2, "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"], "datasets": 1, "source": "API"}',
            id="O3-IS",
        ),
    ),
)
def test_ParquetData_payload(
    pollutants: frozenset[str],
    country: str,
    city: str | None,
    historical: str,
    verified: str,
    unverified: str,
):
    assert (
        json.dumps(
            ParquetData(country, Dataset.Historical, pollutants, city).payload()
        )
        == historical
    ), "unexpected historical info"
    assert (
        json.dumps(
            ParquetData(country, Dataset.Verified, pollutants, city).payload()
        )
        == verified
    ), "unexpected verified info"
    assert (
        json.dumps(
            ParquetData(country, Dataset.Unverified, pollutants, city).payload()
        )
        == unverified
    ), "unexpected unverified info"


@pytest.mark.asyncio
async def test_Client_country(client: Client):
    async with client:
        payload = await client.country()

    assert len(payload) == len(DB.COUNTRY_CODES)

    country_codes = set(country["countryCode"] for country in payload)
    assert country_codes == DB.COUNTRY_CODES


@pytest.mark.asyncio
async def test_Client_property(client: Client):
    async with client:
        payload = await client.property()

    # some pollutants have more than one ID
    assert len(payload) >= len(DB.POLLUTANTS)

    notations = set(pollutant["notation"] for pollutant in payload)
    assert notations == DB.POLLUTANTS


@pytest.mark.asyncio
async def test_Client_city(client: Client):
    async with client:
        payload = await client.city(tuple(DB.COUNTRY_CODES))

    # come countries have no cities
    country_codes = set(country["countryCode"] for country in payload)
    assert country_codes <= DB.COUNTRY_CODES
    assert country_codes == DB.COUNTRY_CODES - {
        "AD", "AL", "BA", "GI", "LI", "ME", "MK", "RS", "TR", "XK",
    }  # fmt: skip


@pytest.mark.asyncio
async def test_Client_download_urls(client: Client):
    info = ParquetData("MT", Dataset.Historical, None, "Valletta")
    async with client:
        text = await client.download_urls(info.payload())

    header, *urls = text.splitlines()
    assert header == "ParquetFileUrl"
    assert len(urls) == 22

    regex = re.compile(rf"https://.*/{info.country}/.*\.parquet")
    for url in urls:
        assert regex.match(url) is not None, f"wrong {url=} start"


@pytest.mark.asyncio
async def test_Client_download_binary(tmp_path: Path, client: Client):
    urls = {
        "https://data_is_here.eu/FI/data.parquet": tmp_path / "FI.parquet",
        "https://data_is_here.eu/NO/data.parquet": tmp_path / "NO.parquet",
        "https://data_is_here.eu/SE/data.parquet": tmp_path / "SE.parquet",
        "https://data_is_here.eu/MT/data.parquet": tmp_path / "MT.parquet",
    }
    assert not tuple(tmp_path.glob("*.parquet"))
    async with client:
        for url, path in urls.items():
            assert await client.download_binary(url, path) == path
            assert path.is_file()

    assert len(tuple(tmp_path.glob("*.parquet"))) == len(urls)


@pytest.mark.asyncio
async def test_Client_download_metadata(tmp_path: Path, client: Client):
    path = tmp_path / "metadata.csv"
    assert not path.is_file()
    async with client:
        assert await client.download_metadata(path) == path
        assert path.is_file()

    with path.open() as file:
        assert len(file.readlines()) == 276


@pytest.mark.asyncio
async def test_Session_country(session: Session):
    async with session:
        country_codes = await session.countries

    assert (
        len(country_codes) == len(set(country_codes)) == len(DB.COUNTRY_CODES)
    )
    assert set(country_codes) == DB.COUNTRY_CODES


@pytest.mark.asyncio
async def test_Session_pollutants(session: Session):
    async with session:
        pollutants = await session.pollutants

    assert pollutants == DB.pollutants()


@pytest.mark.asyncio
async def test_Session_city(session: Session):
    async with session:
        cities = await session.cities()

    # come countries have no cities
    assert cities.keys() <= DB.COUNTRY_CODES
    assert cities.keys() == DB.COUNTRY_CODES - {
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
async def test_Session_url_to_files(session: Session):
    info = ParquetData("MT", Dataset.Historical, None, "Valletta")

    assert session.number_of_urls == 0
    async with session:
        await session.url_to_files(info)
        assert session.number_of_urls == 22

        regex = re.compile(rf"https://.*/{info.country}/.*\.parquet")
        for url in session.urls:
            assert regex.match(url) is not None, f"wrong {url=} start"

    assert session.number_of_urls == 0


@pytest.mark.asyncio
async def test_Session_download_to_directory(tmp_path: Path, session: Session):
    assert session.number_of_urls == 0
    session.add_urls(CSV_PARQUET_URLS_RESPONSE.splitlines()[-5:])
    assert session.number_of_urls == 5

    assert not tuple(tmp_path.glob("??/*.parquet"))
    async with session:
        await session.download_to_directory(tmp_path)
        assert session.number_of_urls == 0

    assert len(tuple(tmp_path.glob("??/*.parquet"))) == 5


@pytest.mark.asyncio
async def test_Session_download_metadata(tmp_path: Path, session: Session):
    path = tmp_path / "metadata.csv"
    assert not path.is_file()
    async with session:
        await session.download_metadata(path)
        assert path.is_file()

    with path.open() as file:
        assert len(file.readlines()) == 276


@pytest.mark.asyncio
async def test_download(tmp_path: Path, session: Session):
    assert not tuple(tmp_path.rglob("*.parquet"))
    assert not tuple(tmp_path.rglob("*.csv"))
    await download(
        Dataset.Historical,
        tmp_path,
        countries={"MT"},
        cities={"Valletta"},
        metadata=True,
        session=session,
    )
    assert len(tuple(tmp_path.glob("MT/*.parquet"))) == 22
    assert tmp_path.joinpath("metadata.csv").is_file()
