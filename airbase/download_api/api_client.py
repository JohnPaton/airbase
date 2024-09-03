from __future__ import annotations

from enum import IntEnum
from typing import NamedTuple
from warnings import warn

from ..summary import COUNTRY_CODES, DB
from .api_types import ParquetDataJSON


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


class ParquetData(NamedTuple):
    """
    info needed for requesting the URLs for country and dataset
    the request can be further restricted with the `pollutant` and `city` param
    """

    country: str
    dataset: Dataset
    pollutant: set[str] | None = None
    city: str | None = None
    source: str = "API"  # for EEA internal use

    def __hash__(self) -> int:
        return hash(str(self))

    def payload(self) -> ParquetDataJSON:
        return dict(
            countries=[self.country],
            cities=[] if self.city is None else [self.city],
            properties=[]
            if self.pollutant is None
            else DB.properties(*self.pollutant),
            datasets=[self.dataset],
            source=self.source,
        )


def request_info_by_city(
    dataset: Dataset, *cities, pollutant: set[str] | None = None
) -> set[ParquetData]:
    """download info one city at the time"""
    info: set[ParquetData] = set()
    for city in cities:
        if (country := DB.search_city(city)) is None:
            warn(f"Unknown {city=}, skip", UserWarning, stacklevel=-2)
            continue

        info.add(ParquetData(country, dataset, pollutant, city))

    return info


def request_info_by_country(
    dataset: Dataset, *countries, pollutant: set[str] | None = None
) -> set[ParquetData]:
    """download info one country at the time"""
    info: set[ParquetData] = set()
    for country in countries:
        if country not in COUNTRY_CODES:
            warn(f"Unknown {country=}, skip", UserWarning, stacklevel=-2)
            continue

        info.add(ParquetData(country, dataset, pollutant))

    return info
