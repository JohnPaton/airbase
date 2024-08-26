from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from enum import IntEnum
from pathlib import Path
from types import TracebackType
from typing import Literal, NamedTuple
from warnings import warn

import aiocache
import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

COUNTRY_CODES = set(
    """
    AD AL AT BA BE BG CH CY CZ DE DK EE ES FI FR GB GI GR HR HU
    IE IS IT LI LT LU LV ME MK MT NL NO PL PT RO RS SE SI SK TR
    XK
    """.split()
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


class DownloadClient(AbstractAsyncContextManager):
    base_url = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"

    def __init__(
        self,
        timeout: float | None = None,
        max_concurrent: int = 10,
    ) -> None:
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.session: aiohttp.ClientSession | None = None
        self.semaphore: asyncio.Semaphore | None = None

    async def __aenter__(self) -> DownloadClient:
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(self.timeout),
        )
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        assert self.session is not None, "outside context manager"
        await self.session.close()
        self.semaphore = self.session = None

    async def _get_json(
        self,
        entry_point: Literal["/Country", "/Property"],
        *,
        encoding: str | None,
    ) -> list[dict[str, str]]:
        assert self.session is not None, "outside context manager"
        async with self.session.get(
            self.base_url + entry_point, ssl=False
        ) as r:
            r.raise_for_status()
            return await r.json(encoding=encoding)  # type:ignore[no-any-return]

    @aiocache.cached()
    async def countries(self) -> list[str]:
        """request country codes from API"""
        payload = await self._get_json("/Country", encoding="UTF-8")
        return [country["countryCode"] for country in payload]

    @aiocache.cached()
    async def pollutants(self) -> defaultdict[str, set[int]]:
        """requests pollutants id and notation from API"""

        payload = await self._get_json("/Property", encoding="UTF-8")
        ids: defaultdict[str, set[int]] = defaultdict(set)
        for poll in payload:
            key, val = poll["notation"], pollutant_id_from_url(poll["id"])
            ids[key].add(val)
        return ids

    async def _post_json(
        self,
        entry_point: Literal["/City"],
        data: tuple[str, ...] | list[str],
        *,
        encoding: str | None,
    ) -> list[dict[str, str]]:
        assert self.session is not None, "outside context manager"
        async with self.session.post(
            self.base_url + entry_point, json=data, ssl=False
        ) as r:
            r.raise_for_status()
            return await r.json(encoding=encoding)  # type:ignore[no-any-return]

    @aiocache.cached()
    async def cities(self, *countries: str) -> defaultdict[str, set[str]]:
        """city names id and notation from API"""
        if not COUNTRY_CODES.issuperset(countries):
            unknown = sorted(set(countries) - COUNTRY_CODES)
            warn(
                f"Unknown country code(s) {', '.join(unknown)}",
                UserWarning,
                stacklevel=2,
            )

        payload = await self._post_json("/City", countries, encoding="UTF-8")
        cities: defaultdict[str, set[str]] = defaultdict(set)
        for city in payload:
            key, val = city["countryCode"], city["cityName"]
            cities[key].add(val)
        return cities

    async def _text_post(
        self,
        entry_point: Literal["/ParquetFile/urls"],
        urls: tuple[DownloadInfo, ...],
        *,
        encoding: str | None,
        raise_for_status: bool,
    ) -> AsyncIterator[str]:
        """
        multiple post requests to an specific Download API entry point and return decoded text
        from each request as they become available

        :param urls: info about requested urls
        :param encoding: text encoding used for decoding each response's body
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.

        :return: url text or path to downloaded text, one by one as the requests are completed
        """

        async def fetch(info: DownloadInfo) -> str:
            """retrieve text, nothing more"""
            assert self.semaphore is not None, "outside context manager"
            assert self.session is not None, "outside context manager"
            async with self.semaphore:
                async with self.session.post(
                    self.base_url + entry_point,
                    json=info.request_info(),
                    ssl=False,
                ) as r:
                    r.raise_for_status()
                    return await r.text(encoding=encoding)  # type:ignore[no-any-return]

        jobs = tuple(fetch(info) for info in urls)
        for result in asyncio.as_completed(jobs):
            try:
                yield await result
            except asyncio.CancelledError:
                continue
            except aiohttp.ClientResponseError as e:
                if raise_for_status:
                    raise
                warn(str(e), category=RuntimeWarning)

    async def url_to_files(
        self,
        *urls: DownloadInfo,
        progress: bool = False,
        raise_for_status: bool = True,
    ) -> set[str]:
        """
        multiple request for file URLs and return only the unique URLs among all the responses

        :param urls: info about requested urls, should be unique
        :param progress: show progress bar
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.
        :param max_concurrent: maximum concurrent requests

        :return: unique file URLs among from all the responses
        """
        lines: set[str] = set()
        async for text in tqdm(
            self._text_post(
                "/ParquetFile/urls",
                urls,
                encoding="UTF-8",
                raise_for_status=raise_for_status,
            ),
            total=len(urls),
            leave=True,
            disable=not progress,
        ):
            lines.update(
                line.strip()
                for line in text.splitlines()
                if line.strip().startswith("http")
            )
        return lines

    async def _binary_files(
        self,
        urls: dict[str, Path],
        *,
        raise_for_status: bool,
    ) -> AsyncIterator[Path]:
        """
        download multiple files in parallel

        :param urls: mapping between url and download path
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.

        :return: url text or path to downloaded text, one by one as the requests are completed
        """

        async def download(url: str, path: Path) -> Path:
            """retrieve binary and write into path"""
            assert self.semaphore is not None, "outside context manager"
            assert self.session is not None, "outside context manager"
            async with self.semaphore:
                async with self.session.get(url, ssl=False) as r:
                    r.raise_for_status()
                    payload = await r.read()

                async with aiofiles.open(path, mode="wb") as f:
                    await f.write(payload)

                return path

        jobs = tuple(download(url, path) for url, path in urls.items())
        for result in asyncio.as_completed(jobs):
            try:
                yield await result
            except asyncio.CancelledError:
                continue
            except aiohttp.ClientResponseError as e:
                if raise_for_status:
                    raise
                warn(str(e), category=RuntimeWarning)

    async def download_to_directory(
        self,
        root_path: Path,
        *urls: str,
        skip_existing: bool = True,
        progress: bool = False,
        raise_for_status: bool = True,
    ) -> None:
        """
        download into a directory, files for different counties are kept on different sub directories

        :param root_path: The directory to save files in (must exist)
        :param urls: urls to files to download
        :param skip_existing: (optional) Don't re-download files if they exist in `root_path`.
            If False, existing files in `root_path` may be overwritten.
            Empty files will be re-downloaded regardless of this option. Default True.
        :param progress: show progress bar
        :param raise_for_status: (optional) Raise exceptions if
            download links return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead. Default True.
        """

        if not root_path.is_dir():
            raise NotADirectoryError(
                f"{root_path.resolve()} is not a directory."
            )

        url_paths: dict[str, Path] = {
            url: root_path / "/".join(url.split("/")[-2:]) for url in urls
        }
        if skip_existing:
            url_paths = {
                url: path
                for url, path in url_paths.items()
                # re-download empty files
                if not path.is_file() or path.stat().st_size == 0
            }

        # create missing country sum-directories before downloading
        for parent in {path.parent for path in url_paths.values()}:
            parent.mkdir(exist_ok=True)

        async for path in tqdm(
            self._binary_files(url_paths, raise_for_status=raise_for_status),
            initial=len(urls) - len(url_paths),
            total=len(urls),
            leave=True,
            disable=not progress,
        ):
            assert path.is_file(), f"missing {path.name}"


def pollutant_id_from_url(url: str) -> int:
    """
    numeric pollutant id from urls like
        http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1
        http://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/44/view
    """
    if url.endswith("view"):
        return int(url.split("/")[-2])
    return int(url.split("/")[-1])
