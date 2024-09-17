"""
Client for Parquet downloads API v1
https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import TracebackType
from warnings import warn
from zipfile import ZipFile, is_zipfile

import aiofiles
import aiohttp

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover

from .types import (
    CityJSON,
    CountryJSON,
    DownloadSummaryJSON,
    ParquetDataJSON,
    PollutantJSON,
)

API_BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
METADATA_URL = "https://discomap.eea.europa.eu/App/AQViewer/download?fqn=Airquality_Dissem.b2g.measurements&f=csv"
METADATA_ARCHIVE = "DataExtract.csv.zip"


class Client(AbstractAsyncContextManager):
    """
    Handle for requests to Parquet downloads API v1
    https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html
    """

    def __init__(
        self,
        *,
        timeout: float | None = None,
        max_concurrent: int = 10,
    ) -> None:
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.__session: aiohttp.ClientSession | None = None
        self.__semaphore: asyncio.Semaphore | None = None

    async def __aenter__(self) -> Self:
        self.__session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                ssl=False, limit=self.max_concurrent
            ),
            timeout=aiohttp.ClientTimeout(self.timeout),
        )
        self.__semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.__session.close()  # type:ignore[union-attr]
        self.__session = self.__semaphore = None

    @property
    def _session(self) -> aiohttp.ClientSession:
        """private `aiohttp.ClientSession`, only available inside the context manager"""
        if self.__session is None:
            raise RuntimeError(
                f"{self.__class__.__qualname__}._session called outside the context manager"
            )
        return self.__session

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        """private `asyncio.Semaphore`, only available inside the context manager"""
        if self.__semaphore is None:
            raise RuntimeError(
                f"{self.__class__.__qualname__}._semaphore called outside the context manager"
            )
        return self.__semaphore

    async def country(self) -> CountryJSON:
        """get request to /Country"""
        async with self._session.get(f"{API_BASE_URL}/Country") as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def pollutant(self) -> PollutantJSON:
        """get request to /Property"""
        async with self._session.get(f"{API_BASE_URL}/Pollutant") as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def city(self, payload: tuple[str, ...]) -> CityJSON:
        """post request to /City"""
        async with self._session.post(
            f"{API_BASE_URL}/City", json=payload
        ) as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_summary(
        self, payload: ParquetDataJSON
    ) -> DownloadSummaryJSON:
        """post request to /DownloadSummary"""
        async with self._session.post(
            f"{API_BASE_URL}/DownloadSummary", json=payload
        ) as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_urls(self, payload: ParquetDataJSON) -> str:
        """post request to /ParquetFile/urls"""
        async with self._session.post(
            f"{API_BASE_URL}/ParquetFile/urls", json=payload
        ) as r:
            r.raise_for_status()
            return await r.text(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_binary(self, url: str, path: Path) -> Path:
        """get request to `url`, write response body content (in binary form) into a a binary file,
        and return `path` (exactly as the input)"""
        async with self._semaphore:
            async with self._session.get(url) as r:
                r.raise_for_status()
                payload = await r.read()
            async with aiofiles.open(path, mode="wb") as f:
                await f.write(payload)

        return path

    async def download_metadata(self, path: Path) -> Path:
        """download compressed metadata file and returns path to uncompressed csv"""
        archive = await self.download_binary(
            METADATA_URL, path.with_name(METADATA_ARCHIVE)
        )
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, extract_metadata_csv, archive, path
        )


def extract_metadata_csv(archive: Path, metadata: Path) -> Path:
    """extract metadata CSV from zip file"""
    if archive.suffix != ".zip" or not is_zipfile(archive):
        warn(
            f"{archive.name} is not a zip file, skip extraction",
            category=RuntimeWarning,
        )
        return archive

    try:
        path = archive.with_suffix("")  # without the '.zip'
        with ZipFile(archive) as zip:
            zip.extract(path.name, archive.parent)
    except KeyError:
        warn(
            f"{path.name} not in {archive.name}, skip extraction",
            category=RuntimeWarning,
        )
        return archive

    assert path.is_file() and path.stat().st_size > 0
    archive.unlink()

    return path.rename(metadata)
