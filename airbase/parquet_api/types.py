"""
type annotations from
https://eeadmz1-downloads-api-appservice.azurewebsites.net/swagger/index.html
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Literal, TypedDict

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import NotRequired
else:
    from typing_extensions import NotRequired  # pragma:no cover

if sys.version_info >= (3, 10):  # pragma:no cover
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias  # pragma:no cover

if TYPE_CHECKING:
    from .dataset import Dataset


class CityData(TypedDict):
    """part of `/City` response"""

    countryCode: str
    cityName: str


"""full `/City` response"""
CityJSON: TypeAlias = "list[CityData]"


class CountryData(TypedDict):
    """part of `/Country` response"""

    countryCode: str
    countryName: str


"""full `/Country` response"""
CountryJSON: TypeAlias = "list[CountryData]"


class ParquetDataJSON(TypedDict):
    """
    request payload to `/DownloadSummary`, `/ParquetFile` and `/ParquetFile/urls`

    dateTimeStart and dateTimeEnd in yyyy-mm-ddTHH:MM:SSZ format
    aggregationType only for unverified data (dataset: 1)
    """

    countries: list[str]
    cities: list[str]
    properties: list[str]
    dataset: Literal[0, 1, 2] | Dataset
    source: str
    dateTimeStart: NotRequired[str]  #  yyyy-mm-ddTHH:MM:SSZ
    dateTimeEnd: NotRequired[str]  #  yyyy-mm-ddTHH:MM:SSZ
    aggregationType: NotRequired[
        Literal["Hourly data", "Daily data", "Variable intervals"]
    ]


class DownloadSummaryJSON(TypedDict):
    """full `/DownloadSummary` response"""

    numberFiles: int
    size: int


class PollutantDict(TypedDict):
    """part of `Pollutant` response"""

    notation: str
    id: str


"""full `Pollutant` response"""
PollutantJSON: TypeAlias = "list[PollutantDict]"
