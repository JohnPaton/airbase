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
    - CountryCode="" means all countries
    - Source="" means "ALL"
    """

    CountryCode: str
    Pollutant: NotRequired[int]
    Year_from: NotRequired[int]
    Year_to: NotRequired[int]
    Station: NotRequired[str]
    Samplingpoint: NotRequired[str]
    EoICode: NotRequired[str]
    Source: Literal["E1a", "E2a", "ALL"] | Source
    Output: Literal["HTML", "TEXT"] | Output
    CityName: NotRequired[str]
    UpdateDate: NotRequired[str]
    Undelivered: NotRequired[str]
    TimeCoverage: NotRequired[str]
