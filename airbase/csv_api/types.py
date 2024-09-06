"""
type annotations from
https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Literal, TypedDict

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import NotRequired
else:
    from typing_extensions import NotRequired  # pragma:no cover


if TYPE_CHECKING:
    from .dataset import Output, Source


class CSVDataJSON(TypedDict):
    """
    query params to
    https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw

    NOTE
    -    "&CountryCode=" --> all available countries
    -    "&Pollutant="   --> all available pollutants
    -    "&Source="      --> all available sources
    - no "&CountryCode=" --> no results
    - no "&Pollutant="   --> only SO2 (ID=1)
    - no "&Source="      --> error
    """

    CountryCode: str
    Pollutant: int | Literal[""]
    Year_from: NotRequired[int | Literal[""]]
    Year_to: NotRequired[int | Literal[""]]
    Station: NotRequired[str]
    Samplingpoint: NotRequired[str]
    EoICode: NotRequired[str]
    Source: Literal["E1a", "E2a", "ALL"] | Source
    Output: Literal["HTML", "TEXT"] | Output
    CityName: NotRequired[str]
    UpdateDate: NotRequired[str]
    Undelivered: NotRequired[str]
    TimeCoverage: NotRequired[str]
