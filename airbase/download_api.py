from __future__ import annotations

import asyncio
from collections import defaultdict
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace
from typing import NamedTuple
from urllib import parse
from warnings import warn

import aiofiles
import httpx
from tqdm import tqdm

API_SERVICE_ROOT = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
COUNTRY_CODES = set(
    """
    AD AL AT BA BE BG CH CY CZ DE DK EE ES FI FR GB GI GR HR HU
    IE IS IT LI LT LU LV ME MK MT NL NO PL PT RO RS SE SI SK TR
    XK
    """.split()
)

DEFAULT = SimpleNamespace(
    encoding="UTF-8",
    progress=False,
    raise_for_status=True,
    max_concurrent=10,
)


class Dataset(IntEnum):
    """
    1. Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the
    beginning of 2023.
    2. Verified data (E1a) from 2013 to 2022 reported by countries by 30 September each
    year for the previous year.
    3. Historical Airbase data delivered between 2002 and 2012 before Air Quality
    Directive 2008/50/EC entered into force.

    https://eeadmz1-downloads-webapp.azurewebsites.net/content/documentation/How_To_Downloads.pdf
    """

    Historical = Airbase = 3
    Verified = E1a = 2
    Unverified = UDT = E2a = 1

    def __str__(self) -> str:
        return self.name


class DownloadInfo(NamedTuple):
    """
    info needed for requesting the URLs for one pollutant from one country and dataset
    the request can be further restricted with the `cities` param
    """

    pollutant: str
    country: str
    dataset: Dataset
    cities: tuple[str, ...] | None = None
    source: str = "API"  # for EEA internal use

    def request_info(self) -> dict[str, list[str] | list[Dataset] | str]:
        return dict(
            countries=[self.country],
            cities=[] if self.cities is None else list(self.cities),
            properties=[self.pollutant],
            datasets=[self.dataset],
            source=self.source,
        )

    @classmethod
    def historical(
        cls, pollutant: str, country: str, *cities: str
    ) -> DownloadInfo:
        return cls(pollutant, country, Dataset.Historical, cities)

    @classmethod
    def verified(
        cls, pollutant: str, country: str, *cities: str
    ) -> DownloadInfo:
        return cls(pollutant, country, Dataset.Verified, cities)

    @classmethod
    def unverified(
        cls, pollutant: str, country: str, *cities: str
    ) -> DownloadInfo:
        return cls(pollutant, country, Dataset.Unverified, cities)


async def _raise_for_status(response: httpx.Response):
    response.raise_for_status()


async def _warn_for_status(response: httpx.Response):
    if response.status_code >= 400:
        warn(
            f"HTTP Error {response.status_code} for URL {response.url}",
            category=RuntimeWarning,
        )


class DownloadAPIClient(httpx.AsyncClient):
    def __init__(
        self,
        progress=DEFAULT.progress,
        raise_for_status=DEFAULT.raise_for_status,
        **kwargs,
    ):
        defaults = dict(
            base_url=API_SERVICE_ROOT,
            event_hooks=dict(request=[], response=[]),
            timeout=60,
        )

        if raise_for_status:
            defaults["event_hooks"]["response"].append(_raise_for_status)
        else:
            defaults["event_hooks"]["response"].append(_warn_for_status)

        defaults.update(kwargs)
        super().__init__(**defaults)
        self.progress = progress

    async def raw_country_json(self):
        """
        Get list of country codes
        """
        response = await self.get("/Country")
        return response.json()

    async def raw_property_json(self):
        """
        Get list of pollutants and notation
        """
        response = await self.get("/Property")
        return response.json()

    async def raw_city_json(self, *countries: str):
        response = await self.post("/City", json=countries)
        return response.json()

    async def parquet_file_urls(self, *urls: DownloadInfo):
        """
        Get the Parquet file URLs matching all the provided DownloadInfo
        """
        jobs = [
            self.post("/ParquetFile/urls", json=info.request_info())
            for info in urls
        ]

        for response in tqdm(
            asyncio.as_completed(jobs),
            total=len(jobs),
            leave=True,
            disable=not self.progress,
        ):
            response = await response
            lines = [line.strip() for line in response.text.split("\n")]
            for line in lines:
                if line.startswith("http"):
                    yield line

    async def pollutants(self) -> defaultdict[str, set[int]]:
        """
        Get pollutant notation and ids
        """
        payload = await self.raw_property_json()
        ids: defaultdict[str, set[int]] = defaultdict(set)
        for poll in payload:
            key, val = poll["notation"], pollutant_id_from_url(poll["id"])
            ids[key].add(val)
        return ids

    async def countries(self) -> list[str]:
        """
        Get list of all countries
        """
        payload = await self.raw_country_json()
        return [country["countryCode"] for country in payload]

    async def cities(self, *countries) -> defaultdict[str, set[str]]:
        """
        Get list of cities in each country
        """
        if not COUNTRY_CODES.issuperset(countries):
            unknown = sorted(set(countries) - COUNTRY_CODES)
            warn(
                f"Unknown country code(s) {', '.join(unknown)}",
                UserWarning,
                stacklevel=2,
            )

        payload = await self.raw_city_json(*countries)
        cities: defaultdict[str, set[str]] = defaultdict(set)
        for city in payload:
            key, val = city["countryCode"], city["cityName"]
            cities[key].add(val)

        return cities

    async def _download_url_to_directory(
        self, url: str, destination: str | Path
    ):
        """
        Download the content of a list of URLs ot files in a directory
        """
        destination = Path(destination)
        response = await self.get(url)
        url_path = parse.urlparse(url).path
        filename = Path(url_path).name
        async with aiofiles.open(destination / filename, "wb") as f:
            await f.write(response.content)

    async def download(self, *urls: DownloadInfo, destination: str | Path):
        # TODO: Progress Bar
        jobs = []
        async for parquet_url in self.parquet_file_urls(*urls):
            jobs.append(
                self._download_url_to_directory(parquet_url, destination)
            )

        await asyncio.gather(*jobs)


_CLIENT: DownloadAPIClient | None = None


def get_client() -> DownloadAPIClient:
    global _CLIENT
    if not _CLIENT:
        _CLIENT = DownloadAPIClient(
            verify=False,
            limits=httpx.Limits(max_connections=DEFAULT.max_concurrent),
            base_url=API_SERVICE_ROOT,
        )
    return _CLIENT


def countries() -> list[str]:
    """request country codes from API"""
    client = get_client()
    return asyncio.get_event_loop().run_until_complete(client.countries())


def pollutant_id_from_url(url: str) -> int:
    """
    numeric pollutant id from urls like
        http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1
        http://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/44/view
    """
    if url.endswith("view"):
        return int(url.split("/")[-2])
    return int(url.split("/")[-1])


def pollutants() -> defaultdict[str, set[int]]:
    """requests pollutants id and notation from API"""
    client = get_client()
    return asyncio.get_event_loop().run_until_complete(client.pollutants())


def cities(*countries: str) -> defaultdict[str, set[str]]:
    """city names id and notation from API"""
    client = get_client()
    return asyncio.get_event_loop().run_until_complete(
        client.cities(*countries)
    )
