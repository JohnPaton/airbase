"""
Abstract Client for Parquet downloads API v1
https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html
"""

from __future__ import annotations

import sys
from abc import abstractmethod
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TypedDict

if sys.version_info >= (3, 10):  # pragma:no cover
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias  # pragma:no cover


class CityDict(TypedDict):
    countryCode: str
    cityName: str


CityResponse: TypeAlias = "list[CityDict]"


class CountryDict(TypedDict):
    countryCode: str


CountryResponse: TypeAlias = "list[CountryDict]"


class DownloadSummaryDict(TypedDict):
    numberFiles: int
    size: int


DownloadSummaryResponse: TypeAlias = DownloadSummaryDict


class ParquetDataDict(TypedDict):
    countries: list[str]
    cities: list[str]
    properties: list[str]
    datasets: list[int]
    source: str


ParquetDataRequest: TypeAlias = ParquetDataDict


class PropertyDict(TypedDict):
    notation: str
    id: str


PropertyResponse: TypeAlias = "list[PropertyDict]"


class AbstractClient(AbstractAsyncContextManager):
    """
    ABC for requests to Parquet downloads API v1
    https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html

    Limiting the number or actives requests is part of the concrete implementation
    """

    base_url = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"

    @abstractmethod
    async def city(self, data: tuple[str, ...]) -> CityResponse:
        """Single post request to /City"""

    @abstractmethod
    async def country(self) -> CountryResponse:
        """Single get request to /Country"""

    @abstractmethod
    async def property(self) -> PropertyResponse:
        """Single get request to /Property"""

    @abstractmethod
    async def download_summary(
        self, data: ParquetDataRequest
    ) -> DownloadSummaryResponse:
        """Single post request to /DownloadSummary"""

    @abstractmethod
    async def download_urls(self, data: ParquetDataRequest) -> str:
        """Single post request to /ParquetFile/urls"""

    @abstractmethod
    async def download_binary(self, url: str, path: Path) -> Path:
        """Single get request to `url`, write response body content (in binary form) into a a binary file,
        and return `path` (exactly as the input)"""
