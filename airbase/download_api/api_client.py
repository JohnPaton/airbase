from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from enum import IntEnum
from pathlib import Path
from types import TracebackType
from typing import Literal, NamedTuple, TypedDict, overload
from warnings import warn

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover


import aiofiles
import aiohttp


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

    def __str__(self) -> str:  # pragma:no cover
        return self.name


class DownloadInfo(NamedTuple):
    """
    info needed for requesting the URLs for one pollutant from one country and dataset
    the request can be further restricted with the `city` param
    """

    pollutant: str | None
    country: str | None
    dataset: Dataset
    city: str | None = None
    source: str = "API"  # for EEA internal use

    def request_info(self) -> dict[str, list[str] | list[Dataset] | str]:
        return dict(
            countries=[] if self.country is None else [self.country],
            cities=[] if self.city is None else [self.city],
            properties=[] if self.pollutant is None else [self.pollutant],
            datasets=[self.dataset],
            source=self.source,
        )


class CityDict(TypedDict):
    countryCode: str
    cityName: str


class CountryDict(TypedDict):
    countryCode: str


class DownloadSummaryDict(TypedDict):
    numberFiles: int
    size: int


class PropertyDict(TypedDict):
    notation: str
    id: str


class DownloadAPI(AbstractAsyncContextManager):
    base_url = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"

    def __init__(
        self,
        *,
        custom_base_url: str | None = None,
        timeout: float | None = None,
        max_concurrent: int = 10,
    ) -> None:
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> Self:
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.max_concurrent),
            timeout=aiohttp.ClientTimeout(self.timeout),
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        assert self.session is not None, "outside context manager"
        await self.session.close()

    @overload
    async def _get(
        self, entry_point: Literal["/Country"], *, encoding: str | None
    ) -> list[CountryDict]: ...

    @overload
    async def _get(
        self, entry_point: Literal["/Property"], *, encoding: str | None
    ) -> list[PropertyDict]: ...

    async def _get(self, entry_point, *, encoding):
        """single get request"""
        assert self.session is not None, "outside context manager"
        async with self.session.get(
            self.base_url + entry_point, ssl=False
        ) as r:
            r.raise_for_status()
            return await r.json(encoding=encoding)

    async def country(self) -> list[CountryDict]:
        """get request to /Country end point"""
        return await self._get("/Country", encoding="UTF-8")

    async def property(self) -> list[PropertyDict]:
        """get request to /Property end point"""
        return await self._get("/Property", encoding="UTF-8")

    async def _post(
        self,
        entry_point: Literal["/City"],
        data: tuple[str, ...],
        *,
        encoding: str | None,
    ) -> list[CityDict]:
        """single post request"""
        assert self.session is not None, "outside context manager"
        async with self.session.post(
            self.base_url + entry_point, json=data, ssl=False
        ) as r:
            r.raise_for_status()
            return await r.json(encoding=encoding)  # type:ignore[no-any-return]

    async def city(self, data: tuple[str, ...]) -> list[CityDict]:
        """post request to /City end point"""
        return await self._post("/City", data, encoding="UTF-8")

    async def __post_multi(
        self,
        entry_point: Literal["/DownloadSummary", "/ParquetFile/urls"],
        urls: set[DownloadInfo],
        *,
        encoding: str | None,
        raise_for_status: bool,
    ) -> AsyncIterator[str]:
        """multiple post requests in parallel"""

        async def fetch(info: DownloadInfo) -> str:
            """retrieve text, nothing more"""
            assert self.session is not None, "outside context manager"
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
            except asyncio.CancelledError:  # pragma:no cover
                continue
            except aiohttp.ClientResponseError as e:  # pragma:no cover
                if raise_for_status:
                    raise
                warn(str(e), category=RuntimeWarning)

    async def download_summary(
        self, data: set[DownloadInfo], raise_for_status: bool
    ) -> AsyncIterator[DownloadSummaryDict]:
        """
        multiple post request to /DownloadSummary end point in parallel
        yields one decoded result at the time
        """
        async for text in self.__post_multi(
            "/DownloadSummary",
            data,
            encoding="UTF-8",
            raise_for_status=raise_for_status,
        ):
            yield json.loads(text)

    async def download_urls(
        self,
        data: set[DownloadInfo],
        raise_for_status: bool,
    ) -> AsyncIterator[set[str]]:
        """
        multiple post request to /ParquetFile/urls end point in parallel
        yields unique download url from each requests as they are completed
        """
        async for text in self.__post_multi(
            "/ParquetFile/urls",
            data,
            encoding="UTF-8",
            raise_for_status=raise_for_status,
        ):
            lines = (line.strip() for line in text.splitlines())
            yield set(
                line
                for line in lines
                if line.startswith(("http://", "https://"))
            )

    async def download_binary_files(
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

        :return: path to downloaded text, one by one as the requests are completed
        """

        async def download(url: str, path: Path) -> Path:
            """retrieve binary and write into path"""
            assert self.session is not None, "outside context manager"
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
            except asyncio.CancelledError:  # pragma:no cover
                continue
            except aiohttp.ClientResponseError as e:  # pragma:no cover
                if raise_for_status:
                    raise
                warn(str(e), category=RuntimeWarning)
