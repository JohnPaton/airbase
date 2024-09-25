from __future__ import annotations

from enum import Enum, IntEnum
from typing import NamedTuple
from warnings import warn

from ..summary import DB
from .types import ParquetDataJSON


class Dataset(IntEnum):
    """
    1. Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the
    beginning of 2024.
    2. Verified data (E1a) from 2013 to 2023 reported by countries by 30 September each
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


class AggregationType(str, Enum):
    """
    represents whether the data collected is obtaining the values:
    1. Hourly data.
    2. Daily data.
    3. Variable intervals (different than the previous observations such as weekly, monthly, etc.)

    https://eeadmz1-downloads-webapp.azurewebsites.net/content/documentation/How_To_Downloads.pdf
    """

    Hourly = "hour"
    Daily = "day"
    Other = VariableIntervals = "var"

    def __str__(self) -> str:
        return self.value


class ParquetData(NamedTuple):
    """
    info needed for requesting the URLs for country and dataset
    the request can be further restricted with the `pollutant`, `city` and `frequency`
    """

    country: str
    dataset: Dataset
    pollutant: frozenset[str] | None = None
    city: str | None = None

    # Optional
    frequency: AggregationType | None = None
    source: str = "API"  # for EEA internal use

    def payload(self) -> ParquetDataJSON:
        payload: ParquetDataJSON = dict(
            countries=[self.country],
            cities=[] if self.city is None else [self.city],
            pollutants=[]
            if self.pollutant is None
            else DB.properties(*self.pollutant),
            dataset=self.dataset,
            source=self.source,
        )

        # Optional
        if self.frequency is not None:
            payload["aggregationType"] = self.frequency

        return payload


def request_info_by_city(
    dataset: Dataset,
    *cities,
    pollutants: frozenset[str] | set[str] | None = None,
    frequency: AggregationType | None = None,
) -> set[ParquetData]:
    """download info one city at the time"""
    if not pollutants:
        pollutants = None
    if isinstance(pollutants, set):
        pollutants = frozenset(pollutants)

    info: set[ParquetData] = set()
    for city in cities:
        if (country := DB.search_city(city)) is None:
            warn(f"Unknown {city=}, skip", UserWarning, stacklevel=-2)
            continue

        info.add(ParquetData(country, dataset, pollutants, city, frequency))

    return info


def request_info_by_country(
    dataset: Dataset,
    *countries,
    pollutants: frozenset[str] | set[str] | None = None,
    frequency: AggregationType | None = None,
) -> set[ParquetData]:
    """download info one country at the time"""
    if not pollutants:
        pollutants = None
    if isinstance(pollutants, set):
        pollutants = frozenset(pollutants)

    info: set[ParquetData] = set()
    for country in countries:
        if country not in DB.COUNTRY_CODES:
            warn(f"Unknown {country=}, skip", UserWarning, stacklevel=-2)
            continue

        info.add(ParquetData(country, dataset, pollutants, frequency=frequency))

    return info
