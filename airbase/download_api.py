from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from enum import IntEnum
from types import SimpleNamespace
from typing import Literal, NamedTuple
from warnings import warn

import aiohttp
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


async def json_from_get_api(
    entry_point: Literal["Country", "Property"],
    *,
    timeout: float | None = None,
    encoding: str | None = DEFAULT.encoding,
) -> list[dict[str, str]]:
    """
    get request to an specific Download API entry point and return decoded JSON

    :param entry_point: Download API entry point
    :param timeout: maximum time to complete request (seconds)
    :param encoding: text encoding used for decoding the response's body

    :return: decoded JSON from response's body
    """

    timeout_ = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_) as session:
        async with session.get(
            f"{API_SERVICE_ROOT}/{entry_point}", ssl=False
        ) as r:
            r.raise_for_status()
            payload: list[dict[str, str]] = await r.json(encoding=encoding)
            return payload


async def json_from_post_api(
    entry_point: Literal["City"],
    data: tuple[str, ...] | list[str],
    *,
    timeout: float | None = None,
    encoding: str | None = DEFAULT.encoding,
) -> list[dict[str, str]]:
    """
    post request to an specific Download API entry point and return decoded JSON

    :param entry_point: Download API entry point
    :param data:
    :param timeout: maximum time to complete request (seconds)
    :param encoding: text encoding used for decoding the response's body

    :return: decoded JSON from response's body
    """

    timeout_ = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_) as session:
        async with session.post(
            f"{API_SERVICE_ROOT}/{entry_point}", json=data, ssl=False
        ) as r:
            r.raise_for_status()
            payload: list[dict[str, str]] = await r.json(encoding=encoding)
            return payload


async def fetch_text_from_post_api(
    entry_point: Literal["ParquetFile/urls"],
    urls: tuple[DownloadInfo, ...],
    *,
    encoding: str | None = DEFAULT.encoding,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> AsyncIterator[str]:
    """
    multiple post requests to an specific Download API entry point and return decoded text
    from each request as they become available

    :param urls: info about requested urls
    :param encoding: text encoding used for decoding each response's body
    :param progress: show progress bar
    :param raise_for_status: Raise exceptions if download links
        return "bad" HTTP status codes. If False,
        a :py:func:`warnings.warn` will be issued instead.
    :param max_concurrent: maximum concurrent requests

    :return: url text or path to downloaded text, one by one as the requests are completed
    """

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch(info: DownloadInfo) -> str:
            """retrieve text, nothing more"""
            async with semaphore:
                async with session.post(
                    f"{API_SERVICE_ROOT}/{entry_point}",
                    json=info.request_info(),
                    ssl=False,
                ) as r:
                    r.raise_for_status()
                    text: str = await r.text(encoding=encoding)
                    return text

        with tqdm(total=len(urls), leave=True, disable=not progress) as p_bar:
            jobs = tuple(fetch(info) for info in urls)
            for result in asyncio.as_completed(jobs):
                p_bar.update(1)
                try:
                    yield await result
                except asyncio.CancelledError:
                    continue
                except aiohttp.ClientResponseError as e:
                    if raise_for_status:
                        raise
                    warn(str(e), category=RuntimeWarning)


def countries() -> list[str]:
    """request country codes from API"""
    payload = asyncio.run(json_from_get_api("Country"))
    return [country["countryCode"] for country in payload]


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
    payload = asyncio.run(json_from_get_api("Property"))
    ids: defaultdict[str, set[int]] = defaultdict(set)
    for poll in payload:
        key, val = poll["notation"], pollutant_id_from_url(poll["id"])
        ids[key].add(val)
    return ids


def cities(*countries: str) -> defaultdict[str, set[str]]:
    """city names id and notation from API"""
    if not COUNTRY_CODES.issuperset(countries):
        unknown = sorted(set(countries) - COUNTRY_CODES)
        warn(
            f"Unknown country code(s) {', '.join(unknown)}",
            UserWarning,
            stacklevel=2,
        )

    payload = asyncio.run(json_from_post_api("City", countries))
    cities: defaultdict[str, set[str]] = defaultdict(set)
    for city in payload:
        key, val = city["countryCode"], city["cityName"]
        cities[key].add(val)
    return cities


def url_to_files(
    *urls: DownloadInfo,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> set[str]:
    """
    multiple request for file URLs and return only the unique URLs among all the responses

    :param urls: info about requested urls
    :param progress: show progress bar
    :param raise_for_status: Raise exceptions if download links
        return "bad" HTTP status codes. If False,
        a :py:func:`warnings.warn` will be issued instead.
    :param max_concurrent: maximum concurrent requests

    :return: unique file URLs among from all the responses
    """

    async def fetch() -> set[str]:
        lines: set[str] = set()
        async for text in fetch_text_from_post_api(
            "ParquetFile/urls",
            urls,
            progress=progress,
            raise_for_status=raise_for_status,
            max_concurrent=max_concurrent,
        ):
            lines.update(
                line.strip()
                for line in text.splitlines()
                if line.strip().startswith("http")
            )
        return lines

    return asyncio.run(fetch())
