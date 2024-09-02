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


class CityData(TypedDict):
    """part of `/City` response"""

    countryCode: str
    cityName: str


"""full `/City` response"""
CityJSON: TypeAlias = "list[CityData]"


class CountryData(TypedDict):
    """part of `/Country` response"""

    countryCode: str


"""full `/Country` response"""
CountryJSON: TypeAlias = "list[CountryData]"


class ParquetDataJSON(TypedDict):
    """request payload to `/DownloadSummary`, `/ParquetFile` and `/ParquetFile/urls`"""

    countries: list[str]
    cities: list[str]
    properties: list[str]
    datasets: list[int]
    source: str


class DownloadSummaryJSON(TypedDict):
    """full `/DownloadSummary` response"""

    numberFiles: int
    size: int


class PropertyDict(TypedDict):
    """part of `Property` response"""

    notation: str
    id: str


"""full `Property` response"""
PropertyJSON: TypeAlias = "list[PropertyDict]"


class AbstractClient(AbstractAsyncContextManager):
    """
    ABC for requests to Parquet downloads API v1
    https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html

    Limiting the number or active requests is part of the concrete implementation
    """

    base_url = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"

    @abstractmethod
    async def city(self, payload: tuple[str, ...]) -> CityJSON:
        """Single post request to /City"""

    @abstractmethod
    async def country(self) -> CountryJSON:
        """Single get request to /Country"""

    @abstractmethod
    async def property(self) -> PropertyJSON:
        """Single get request to /Property"""

    @abstractmethod
    async def download_summary(
        self, payload: ParquetDataJSON
    ) -> DownloadSummaryJSON:
        """Single post request to /DownloadSummary"""

    @abstractmethod
    async def download_urls(self, payload: ParquetDataJSON) -> str:
        """Single post request to /ParquetFile/urls"""

    @abstractmethod
    async def download_binary(self, url: str, path: Path) -> Path:
        """Single get request to `url`, write response body content (in binary form) into a a binary file,
        and return `path` (exactly as the input)"""
