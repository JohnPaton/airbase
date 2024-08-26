from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, AsyncIterator, Coroutine, Literal, NamedTuple
from urllib import parse
from warnings import warn

import aiocache
import aiofiles
import httpx
from tqdm import tqdm

from .util import string_safe_list

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

LOGGER = logging.getLogger(__name__)


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
    try:
        response.raise_for_status()
    except httpx.HTTPError:
        await response.aread()
        LOGGER.error(
            f"HTTP {response.status_code}: Failed to fetch {response.url}. "
            f"Response content:\n{response.text}"
        )
        raise


async def _warn_for_status(response: httpx.Response):
    if response.status_code >= 400:
        warn(
            f"HTTP Error {response.status_code} for URL {response.url}",
            category=RuntimeWarning,
        )


class DownloadAPIClient(httpx.AsyncClient):
    def __init__(
        self,
        progress: bool = DEFAULT.progress,
        raise_for_status: bool = DEFAULT.raise_for_status,
        max_concurrent: int = DEFAULT.max_concurrent,
        **kwargs,
    ):
        """
        Client for the EEA Air Quality Download Service API

        All methods are async.

        https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html

        :param progress: Show progress bar for download
        :param raise_for_status: Raise exceptions for HTTP issues (else just warn)
        :param max_concurrent: Max number of concurrent connections to service
        :param kwargs: Additional kwargs for `httpx.AsyncClient`
        """
        defaults = dict(
            base_url=API_SERVICE_ROOT,
            event_hooks=dict(request=[], response=[]),
            timeout=60,
            limits=httpx.Limits(
                max_connections=max_concurrent,
            ),
            verify=False,
        )

        if raise_for_status:
            defaults["event_hooks"]["response"].append(_raise_for_status)
        else:
            defaults["event_hooks"]["response"].append(_warn_for_status)

        defaults.update(kwargs)
        super().__init__(**defaults)
        self.progress = progress

    @aiocache.cached()
    async def raw_country_json(self) -> list[dict[str, str]]:
        """
        Get list of country codes
        """
        response = await self.get("/Country")
        return response.json()  # type: ignore[no-any-return]

    @aiocache.cached()
    async def raw_property_json(self) -> list[dict[str, str]]:
        """
        Get list of pollutants and notation
        """
        response = await self.get("/Property")
        return response.json()  # type: ignore[no-any-return]

    @aiocache.cached()
    async def raw_city_json(self, *countries: str) -> list[dict[str, str]]:
        response = await self.post("/City", json=countries)
        return response.json()  # type: ignore[no-any-return]

    async def raw_download_summary_json(
        self, url: DownloadInfo
    ) -> dict[str, int]:
        response = await self.post("/DownloadSummary", json=url.request_info())
        return response.json()  # type: ignore[no-any-return]

    async def total_num_parquet_files(self, *urls: DownloadInfo) -> int:
        """
        The total number of parquet files that will be downloaded for a set of URLs
        """
        total_files = 0
        summary_jobs = [self.raw_download_summary_json(url) for url in urls]

        for future in asyncio.as_completed(summary_jobs):
            result: dict[str, int] = await future
            total_files += result["numberFiles"]

        return total_files

    async def parquet_file_urls(
        self, *urls: DownloadInfo
    ) -> AsyncIterator[str]:
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
            disable=not self.progress or len(jobs) <= 1,
            desc="Fetching Parquet file URLs",
            position=0,
        ):
            response = await response
            lines = [line.strip() for line in response.text.split("\n")]
            for line in lines:
                if line.startswith("http"):
                    yield line

    @aiocache.cached()
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

    @aiocache.cached()
    async def countries(self) -> list[str]:
        """
        Get list of all countries
        """
        payload = await self.raw_country_json()
        return [country["countryCode"] for country in payload]

    @aiocache.cached()
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

    @aiocache.cached()
    async def _pollutant_notation_to_properties(
        self, pollutant_notation: str
    ) -> list[str]:
        pollutants = await self.pollutants()
        ids = pollutants[pollutant_notation]
        return [
            f"http://dd.eionet.europa.eu/vocabulary/aq/pollutant/{id}"
            for id in ids
        ]

    async def download_downloadinfo(
        self, *urls: DownloadInfo, destination: str | Path
    ):
        download_jobs = []

        pbar = tqdm(
            leave=True,
            disable=not self.progress,
            desc="Downloading parquets",
            unit="files",
            position=1,
        )
        pbar.set_postfix_str("Checking total number of files..")

        async for parquet_url in self.parquet_file_urls(*urls):
            task = asyncio.create_task(
                self._download_url_to_directory(parquet_url, destination)
            )

            def update_pbar(future):
                pbar.update(1)

            task.add_done_callback(update_pbar)
            download_jobs.append(task)

        if self.progress:
            total_files_task = asyncio.create_task(
                self.total_num_parquet_files(*urls)
            )

            def update_pbar_total(future):
                pbar.total = future.result()
                pbar.set_postfix_str("")

            total_files_task.add_done_callback(update_pbar_total)

        for result in asyncio.as_completed(download_jobs):
            await result

    async def download(
        self,
        destination: str | Path,
        countries: str | list[str] | None = None,
        pollutants: str | list[str] | None = None,
        cities: str | list[str] | None = None,
        datasets: Dataset
        | Literal["Historical", "Verified", "Unverified"]
        | list[
            Dataset | Literal["Historical", "Verified", "Unverified"]
        ] = "Verified",
    ):
        """
        Download all parquet files matching the provided filters.

        If a filter is not provided, all matching values will be
        downloaded.


        :param destination: The directory to save the parquet files in
        :param countries: The country or countries to filter on
        :param pollutants: The pollutant or pollutants to filter on.
            Pollutants should be specified in terms of "notation",
            for example "O3", "NO2". Check DownloadAPIClient.pollutants()
            for available pollutants.
        :param cities: The cities to filter on. Only cities that are
            actually available in each country will be used for that
            country. Check DownloadAPIClient.cities(*countries)
            for available cities.
        :param datasets: The datasets to download ("Historical",
            "Verified", or "Unverified"). Default "Verified".
        """
        countries = countries or await self.countries()
        cities = cities or []
        datasets = datasets or ["Historical", "Verified", "Unverified"]

        available_cities = defaultdict(set)
        if cities:
            available_cities = await self.cities(*string_safe_list(countries))

        downloadinfos: list[DownloadInfo] = []
        matched_cities: set[str] = set()
        for country in string_safe_list(countries):
            for dataset in string_safe_list(datasets):  # type: ignore[arg-type]
                for pollutant in string_safe_list(pollutants):
                    if isinstance(dataset, Dataset):
                        ds = dataset
                    else:
                        ds = Dataset[dataset.title()]  # type: ignore[unreachable]

                    pollutant_properties = (
                        await self._pollutant_notation_to_properties(pollutant)
                        if pollutant
                        else [None]
                    )

                    for property in pollutant_properties:
                        # restrict cities list to cities for this country only
                        if cities:
                            country_cities = set(
                                string_safe_list(cities)
                            ).intersection(available_cities[country])
                            matched_cities = matched_cities.union(
                                country_cities
                            )
                        else:
                            country_cities: set[str] = set()  # type: ignore[no-redef]

                        info = DownloadInfo(
                            country=country,
                            dataset=ds,
                            pollutant=property,
                            cities=tuple(country_cities),
                        )

                        downloadinfos.append(info)

        unmatched_cities = set(string_safe_list(cities)) - matched_cities
        if unmatched_cities:
            warn(
                f"Cities {unmatched_cities} were "
                f"not found for any of the selected countries, so they will not be "
                f"used to filter data. Check available cities using `cities(*countries)`",
                category=UserWarning,
            )

        await self.download_downloadinfo(
            *downloadinfos, destination=destination
        )


_CLIENT: DownloadAPIClient | None = None


def get_client(
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> DownloadAPIClient:
    """
    Get global client for the EEA Air Quality Download Service API.

    Subsequent calls to this function will return the same client instance.

    All client methods are async.

    :param progress: Show progress bar for download
    :param raise_for_status: Raise exceptions for HTTP issues (else just warn)
    :param max_concurrent: Max number of concurrent connections to service
    :return: the global client
    """
    global _CLIENT
    if not _CLIENT:
        _CLIENT = DownloadAPIClient(
            progress=progress,
            raise_for_status=raise_for_status,
            max_concurrent=max_concurrent,
        )
    return _CLIENT


_EVENT_LOOP: asyncio.AbstractEventLoop | None = None


def get_event_loop() -> asyncio.AbstractEventLoop:
    """Get or create a running event loop"""
    global _EVENT_LOOP
    if _EVENT_LOOP is None or _EVENT_LOOP.is_closed():
        _EVENT_LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(_EVENT_LOOP)
    return _EVENT_LOOP


def run_sync(coro: asyncio.Future | Coroutine) -> Any:
    """Run an async method synchronously"""
    return get_event_loop().run_until_complete(coro)


def countries() -> list[str]:
    """request country codes from API"""
    client = get_client()
    return run_sync(client.countries())  # type: ignore


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
    return run_sync(client.pollutants())  # type: ignore


def cities(*countries: str) -> defaultdict[str, set[str]]:
    """city names id and notation from API"""
    client = get_client()
    return run_sync(client.cities(*countries))  # type: ignore


def download_from_downloadinfo(*urls: DownloadInfo, destination: str | Path):
    """
    Download the parquet files matching the given set of DownloadInfo
    """
    client = get_client()
    return run_sync(
        client.download_downloadinfo(*urls, destination=destination)
    )


def download(
    destination: str | Path,
    countries: str | list[str] | None = None,
    pollutants: str | list[str] | None = None,
    cities: str | list[str] | None = None,
    datasets: Dataset
    | Literal["Historical", "Verified", "Unverified"]
    | list[
        Dataset | Literal["Historical", "Verified", "Unverified"]
    ] = "Verified",
):
    """
    Download all parquet files matching the provided filters.

    If a filter is not provided, all matching values will be
    downloaded.


    :param destination: The directory to save the parquet files in
    :param countries: The country or countries to filter on
    :param pollutants: The pollutant or pollutants to filter on.
        Pollutants should be specified in terms of "notation",
        for example "O3", "NO2". Check pollutants()
        for available pollutants.
    :param cities: The cities to filter on. Only cities that are
        actually available in each country will be used for that
        country. Check cities(*countries)
        for available cities.
    :param datasets: The datasets to download ("Historical",
        "Verified", or "Unverified").
    """
    client = get_client()
    run_sync(
        client.download(
            destination=destination,
            countries=countries,
            pollutants=pollutants,
            cities=cities,
            datasets=datasets,
        )
    )
