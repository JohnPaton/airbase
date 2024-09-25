"""
Client for Legacy AirQualityExport
https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import TracebackType

import aiofiles
import aiohttp

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover

from .types import CSVDataJSON

FME_URL = "https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw"
FME_HEADERS = {
    "Authorization": "fmetoken token=8f3a54b3e7054080813237004b35694fbff43580",
    "Content-Type": "application/json",
}
METADATA_URL = (
    "http://discomap.eea.europa.eu/map/fme/metadata/PanEuropean_metadata.csv"
)


class Client(AbstractAsyncContextManager):
    """
    Handle for requests to Legacy AirQualityExport
    https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
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

    async def download_urls(self, params: CSVDataJSON) -> str:
        """get request to AirQualityExport"""
        async with self._session.get(
            FME_URL, headers=FME_HEADERS, params=params
        ) as r:
            r.raise_for_status()
            return await r.text(encoding="utf-8-sig")  # type:ignore[no-any-return]

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
        """download metadata file and return `path` (exactly as the input)"""
        return await self.download_binary(METADATA_URL, path)
