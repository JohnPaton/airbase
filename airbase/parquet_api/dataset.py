from __future__ import annotations

from collections.abc import Collection, Iterator
from enum import IntEnum
from typing import NamedTuple
from warnings import warn

from ..summary import DB
from .types import ParquetDataJSON


class Dataset(IntEnum):
    """
    1. Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the
    beginning of 2025.
    2. Verified data (E1a) from 2013 to 2024 reported by countries by 30 September each
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
    the request can be further restricted with the `pollutant` and `city`
    """

    dataset: Dataset
    country: str
    pollutant: frozenset[str] | None = None
    city: str | None = None

    def payload(self) -> ParquetDataJSON:
        return ParquetDataJSON(
            countries=[self.country],
            cities=[] if self.city is None else [self.city],
            pollutants=[] if self.pollutant is None else sorted(self.pollutant),
            dataset=self.dataset,
            source="API",  # for EEA internal use
        )


def __by_city(
    dataset: Dataset,
    cities: set[str],
    pollutants: frozenset[str] | None,
) -> Iterator[ParquetData]:
    """download info one city at the time"""
    for city in cities:
        if (country := DB.search_city(city)) is None:
            warn(f"Unknown {city=}, skip", UserWarning, stacklevel=-2)
            continue

        yield ParquetData(dataset, country, pollutants, city)


def __by_country(
    dataset: Dataset,
    countries: set[str],
    pollutants: frozenset[str] | None,
) -> Iterator[ParquetData]:
    """download info one country at the time"""
    for country in countries:
        if country not in DB.COUNTRY_CODES:
            warn(f"Unknown {country=}, skip", UserWarning, stacklevel=-2)
            continue

        yield ParquetData(dataset, country, pollutants)


def request_info(
    dataset: Dataset,
    *,
    cities: Collection[str] | None = None,
    countries: Collection[str] | None = None,
    pollutants: Collection[str] | None = None,
) -> Iterator[ParquetData]:
    """
    one download info for each city/pollutant xor country/pollutant
    - cities take precednece over countries
    - countries is None or empty container means all countries
    """
    if not pollutants:
        pollutants = None
    else:
        pollutants = frozenset(pollutants)

    if cities:
        yield from __by_city(dataset, set(cities), pollutants)
    else:
        if not countries:
            countries = DB.COUNTRY_CODES
        yield from __by_country(dataset, set(countries), pollutants)
