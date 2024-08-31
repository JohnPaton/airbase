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
        self.session: aiohttp.ClientSession | None = None
        self.semaphore: asyncio.Semaphore | None = None

    async def __aenter__(self) -> Self:
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                ssl=False, limit=self.max_concurrent
            ),
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
        self.session = self.semaphore = None

    async def country(self) -> CountryResponse:
        """get request to /Country"""
        assert self.session is not None, "outside context manager"
        async with self.session.get(self.base_url + "/Country") as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def property(self) -> PropertyResponse:
        """get request to /Property"""
        assert self.session is not None, "outside context manager"
        async with self.session.get(self.base_url + "/Property") as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def city(self, data: tuple[str, ...]) -> CityResponse:
        """post request to /City"""
        assert self.session is not None, "outside context manager"
        async with self.session.post(self.base_url + "/City", json=data) as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_summary(
        self, data: ParquetDataRequest
    ) -> DownloadSummaryResponse:
        """post request to /DownloadSummary"""
        assert self.session is not None, "outside context manager"
        async with self.session.post(
            self.base_url + "/DownloadSummary", json=data
        ) as r:
            r.raise_for_status()
            return await r.json(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_urls(self, data: ParquetDataRequest) -> str:
        """post request to /ParquetFile/urls"""
        assert self.session is not None, "outside context manager"
        async with self.session.post(
            self.base_url + "/ParquetFile/urls", json=data
        ) as r:
            r.raise_for_status()
            return await r.text(encoding="UTF-8")  # type:ignore[no-any-return]

    async def download_binary(self, url: str, path: Path) -> Path:
        """get request to `url`, write response body content (in binary form) into a a binary file,
        and return `path` (exactly as the input)"""

        assert self.session is not None, "outside context manager"
        assert self.semaphore is not None, "outside context manager"
        async with self.semaphore:
            async with self.session.get(url) as r:
                r.raise_for_status()
                payload = await r.read()
            async with aiofiles.open(path, mode="wb") as f:
                await f.write(payload)

        return path
