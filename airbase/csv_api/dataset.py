from __future__ import annotations

from enum import Enum
from itertools import product
from typing import Literal, NamedTuple
from warnings import warn

from ..summary import DB
from .types import CSVDataJSON


class Source(str, Enum):
    """
    E1a: Verified data from 2013 to 2023 reported by countries by 30 September each
    year for the previous year.
    E2a: Unverified data transmitted continuously data from the beginning of 2024.
    ALL: E1a and E2a

    NOTE
    - only 2024 data available -> no E2a data
    """

    Verified = "E1a"
    Unverified = "E2a"
    ALL = "ALL"

    def __str__(self) -> str:  # pragma:no cover
        return self.value


class Output(str, Enum):
    HTML = "HTML"
    TEXT = "TEXT"

    def __str__(self) -> str:  # pragma:no cover
        return self.value


class CSVData(NamedTuple):
    """
    info needed for requesting the URLs for country, source, year and pollutant_id
    the request can be further restricted with the `city` param
    """

    country: str
    pollutant_id: int | Literal[""]
    source: Source
    year: int
    city: str | None = None
    output: Output = Output.TEXT

    def param(self) -> CSVDataJSON:
        payload: CSVDataJSON = dict(
            CountryCode=self.country,
            Pollutant=self.pollutant_id,
            Year_from=self.year,
            Year_to=self.year,
            Source=self.source,
            Output=self.output,
        )
        if self.city is not None:
            payload["CityName"] = self.city
        return payload


def request_info_by_city(
    source: Source,
    year: int,
    *cities: str,
    pollutants: frozenset[str] | set[str] | None = None,
) -> set[CSVData]:
    """download info one city at the time"""
    countries: dict[str, str] = {}
    for city in cities:
        if (country := DB.search_city(city)) is None:
            warn(f"Unknown {city=}, skip", UserWarning, stacklevel=-2)
            continue

        countries[city] = country

    if not pollutants:
        return set(
            CSVData(country, "", source, year, city)
            for city, country in countries.items()
        )

    return set(
        CSVData(country, id, source, year, city)
        for (city, country), id in product(
            countries.items(),
            set(DB.search_pollutants(*pollutants)),
        )
    )


def request_info_by_country(
    source: Source,
    year: int,
    *countries: str,
    pollutants: frozenset[str] | set[str] | None = None,
) -> set[CSVData]:
    """download info one country at the time"""
    for country in set(countries) - DB.COUNTRY_CODES:
        warn(f"Unknown {country=}, skip", UserWarning, stacklevel=-2)

    if not pollutants:
        return set(
            CSVData(country, "", source, year)
            for country in DB.COUNTRY_CODES.intersection(countries)
        )

    return set(
        CSVData(country, id, source, year)
        for country, id in product(
            DB.COUNTRY_CODES.intersection(countries),
            set(DB.search_pollutants(*pollutants)),
        )
    )
