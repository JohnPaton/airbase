"""
Client for Parquet downloads API v1
https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import TracebackType

import aiofiles
import aiohttp

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover

from .abstract_api_client import (
    AbstractAPIClient,
    CityResponse,
    CountryResponse,
    DownloadSummaryResponse,
    ParquetDataRequest,
    PropertyResponse,
)


class Client(AbstractAPIClient):
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

    async def country(self) -> CountryResponse:
        """get request to /Country"""
        async with self._semaphore:
            async with self._session.get(f"{self.base_url}/Country") as r:
                r.raise_for_status()
                return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def property(self) -> PropertyResponse:
        """get request to /Property"""
        async with self._semaphore:
            async with self._session.get(f"{self.base_url}/Property") as r:
                r.raise_for_status()
                return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def city(self, data: tuple[str, ...]) -> CityResponse:
        """post request to /City"""
        async with self._semaphore:
            async with self._session.post(
                f"{self.base_url}/City", json=data
            ) as r:
                r.raise_for_status()
                return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_summary(
        self, data: ParquetDataRequest
    ) -> DownloadSummaryResponse:
        """post request to /DownloadSummary"""
        async with self._semaphore:
            async with self._session.post(
                f"{self.base_url}/DownloadSummary", json=data
            ) as r:
                r.raise_for_status()
                return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_urls(self, data: ParquetDataRequest) -> str:
        """post request to /ParquetFile/urls"""
        async with self._semaphore:
            async with self._session.post(
                f"{self.base_url}/ParquetFile/urls", json=data
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
