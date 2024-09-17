from __future__ import annotations

import sqlite3
import sys
from contextlib import closing, contextmanager
from functools import cached_property
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, NamedTuple

if sys.version_info >= (3, 11):  # pragma: no cover
    from importlib import resources
else:  # pragma: no cover
    import importlib_resources as resources

if TYPE_CHECKING:
    from airbase.parquet_api.types import (
        CityJSON,
        CountryJSON,
        PollutantJSON,
    )


def summary() -> Path:
    source = resources.files(__package__) / "summary.sqlite"
    path: Path
    with resources.as_file(source) as path:
        return path


class Pollutant(NamedTuple):
    notation: str
    id: int


class SummaryDB:
    """
    In DB containing the available country and pollutants

    cached data from
    https://eeadmz1-downloads-api-appservice.azurewebsites.net/City
    https://eeadmz1-downloads-api-appservice.azurewebsites.net/Country
    https://eeadmz1-downloads-api-appservice.azurewebsites.net/Property
    """

    db = sqlite3.connect(f"file:{summary()}?mode=ro", uri=True)

    @classmethod
    @contextmanager
    def cursor(cls) -> Iterator[sqlite3.Cursor]:
        """db cursor as a "self closing" context manager"""
        with closing(cls.db.cursor()) as cur:
            yield cur

    def countries(cls) -> list[str]:
        """
        Unique country codes.

        :return: list of available country codes
        """

        with cls.cursor() as cur:
            cur.execute("SELECT country_code FROM countries;")
            return list(row[0] for row in cur.fetchall())

    @cached_property
    def COUNTRY_CODES(self) -> frozenset[str]:
        """All unique country codes"""
        return frozenset(self.countries())

    def pollutants(self) -> dict[str, set[int]]:
        """
        Pollutant notations and unique ids.

        :return: The available pollutants, as a dictionary with
        with notation as key and IDs as value, e.g. {"NO": {38}, ...}
        """

        with self.cursor() as cur:
            cur.execute("SELECT pollutant, ids FROM pollutant_ids;")
            return {
                pollutant: set(map(int, ids.split(",")))
                for pollutant, ids in cur.fetchall()
            }

    @cached_property
    def POLLUTANTS(self) -> frozenset[str]:
        """All unique pollutant names/notations"""
        return frozenset(self.pollutants())

    @cached_property
    def POLLUTANT_IDS(self) -> frozenset[int]:
        """All unique pollutant IDs"""
        return frozenset(chain.from_iterable(self.pollutants().values()))

    def properties(self, *pollutants: str) -> list[str]:
        """
        Pollutant description URLs

        https://dd.eionet.europa.eu/vocabulary/aq/pollutant
        """
        if not pollutants:
            return []

        with self.cursor() as cur:
            cur.execute(
                f"""
                SELECT definition_url FROM pollutant
                WHERE pollutant in ({",".join("?"*len(pollutants))});
                """,
                pollutants,
            )
            return [url for (url,) in cur]

    def search_pollutant(
        self, query: str, *, limit: int | None = None
    ) -> Iterator[Pollutant]:
        """
        Search for a pollutant's ID number based on its name.

        :param query: The pollutant to search for.
        :param limit: (optional) Max number of results.

        :return: The best pollutant matches, as tuples of notation and ID,
            e.g. ("NO", 38)
        """

        with self.cursor() as cur:
            cur.execute(
                f"""
                SELECT pollutant, pollutant_id FROM pollutants
                WHERE pollutant LIKE ?
                {f"LIMIT {limit}" if limit else ""};
                """,
                (f"%{query}%",),
            )
            for pollutant, pollutant_id in cur.fetchall():
                yield Pollutant(pollutant, pollutant_id)

    def search_pollutants(self, *pollutants: str) -> Iterator[int]:
        """
        Search for a pollutant ID numbers based from exact matches to pollutant names.

        :param pollutants: The pollutant name(s)/notation(s) to search for.

        :return: ID(s) corresponding to the name(s)/notation(s),
            e.g. "NO" --> 38
        """

        with self.cursor() as cur:
            cur.execute(
                f"""
                SELECT pollutant_id FROM pollutants
                WHERE pollutant in ({",".join("?"*len(pollutants))});
                """,
                pollutants,
            )
            for row in cur.fetchall():
                yield row[0]

    def search_city(self, city: str) -> str | None:
        """
        Search for a country code from city name

        :param city: City name.

        :return: country code, e.g. "NO" for "Oslo"
        """

        with self.cursor() as cur:
            cur.execute(
                "SELECT country_code FROM city WHERE city_name IS ?;",
                (city,),
            )
            row: tuple[str] | None = cur.fetchone()
            return None if row is None else row[0]

    def city_json(self) -> CityJSON:
        """
        simulate a request to
        https://eeadmz1-downloads-api-appservice.azurewebsites.net/City
        """
        with self.cursor() as cur:
            cur.execute(
                "SELECT country_code, city_name FROM city WHERE city_name IS NOT NULL;"
            )
            return [
                dict(countryCode=country_code, cityName=city_name)
                for (country_code, city_name) in cur
            ]

    def country_json(self) -> CountryJSON:
        """
        simulate a request to
        https://eeadmz1-downloads-api-appservice.azurewebsites.net/Country
        """
        with self.cursor() as cur:
            cur.execute("SELECT country_code, country_name FROM country;")
            return [
                dict(countryCode=country_code, countryName=country_name)
                for (country_code, country_name) in cur
            ]

    def pollutant_json(self) -> PollutantJSON:
        """
        simulate a request to
        https://eeadmz1-downloads-api-appservice.azurewebsites.net/Pollutant
        """
        with self.cursor() as cur:
            cur.execute("SELECT pollutant, definition_url FROM pollutant;")
            return [
                dict(notation=pollutant, id=definition_url)
                for (pollutant, definition_url) in cur
            ]


DB = SummaryDB()
