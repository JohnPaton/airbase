from __future__ import annotations

import sqlite3
import sys
from contextlib import closing, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, NamedTuple

if sys.version_info >= (3, 11):  # pragma: no cover
    from importlib import resources
else:  # pragma: no cover
    import importlib_resources as resources

if TYPE_CHECKING:
    from airbase.download_api.abstract_api_client import (
        CityResponse,
        CountryResponse,
        PropertyResponse,
    )


def summary() -> Path:
    source = resources.files(__package__) / "summary.sqlite"
    path: Path
    with resources.as_file(source) as path:
        return path


class Pollutant(NamedTuple):
    notation: str
    id: int


class DB:
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

    @classmethod
    def countries(cls) -> list[str]:
        """
        Unique country codes.

        :return: list of available country codes
        """

        with cls.cursor() as cur:
            cur.execute("SELECT country_code FROM countries;")
            return list(row[0] for row in cur.fetchall())

    @classmethod
    def pollutants(cls) -> dict[str, set[int]]:
        """
        Pollutant notations and unique ids.

        :return: The available pollutants, as a dictionary with
        with notation as key and IDs as value, e.g. {"NO": {38}, ...}
        """

        with cls.cursor() as cur:
            cur.execute("SELECT pollutant, ids FROM pollutant_ids;")
            return {
                pollutant: set(map(int, ids.split(",")))
                for pollutant, ids in cur.fetchall()
            }

    @classmethod
    def properties(cls, *pollutants: str) -> list[str]:
        """
        Pollutant description URLs

        https://dd.eionet.europa.eu/vocabulary/aq/pollutant
        """
        if not pollutants:
            return []

        with cls.cursor() as cur:
            cur.execute(
                f"""
                SELECT definition_url FROM property
                WHERE pollutant in ({",".join("?"*len(pollutants))});
                """,
                pollutants,
            )
            return [url for (url,) in cur]

    @classmethod
    def search_pollutant(
        cls, query: str, *, limit: int | None = None
    ) -> Iterator[Pollutant]:
        """
        Search for a pollutant's ID number based on its name.

        :param query: The pollutant to search for.
        :param limit: (optional) Max number of results.

        :return: The best pollutant matches, as a dictionary with
        with notation as keys with IDs as values, e.g. {"NO": {38}, ...}
        """

        with cls.cursor() as cur:
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

    @classmethod
    def search_city(cls, city: str) -> str | None:
        """
        Search for a country code from city name

        :param city: City name.

        :return: country code, e.g. "NO" for "Oslo"
        """

        with cls.cursor() as cur:
            cur.execute(
                "SELECT country_code FROM city WHERE city_name IS ?;",
                (city,),
            )
            row: tuple[str] | None = cur.fetchone()
            return None if row is None else row[0]

    @classmethod
    def city_json(cls) -> CityResponse:
        """
        simulate a request to
        https://eeadmz1-downloads-api-appservice.azurewebsites.net/City
        """
        with cls.cursor() as cur:
            cur.execute(
                "SELECT country_code, city_name FROM city WHERE city_name IS NOT NULL;"
            )
            return [
                dict(countryCode=country_code, cityName=city_name)
                for (country_code, city_name) in cur
            ]

    @classmethod
    def country_json(cls) -> CountryResponse:
        """
        simulate a request to
        https://eeadmz1-downloads-api-appservice.azurewebsites.net/Country
        """
        with cls.cursor() as cur:
            cur.execute("SELECT country_code FROM countries;")
            return [dict(countryCode=country_code) for (country_code,) in cur]

    @classmethod
    def property_json(cls) -> PropertyResponse:
        """
        simulate a request to
        https://eeadmz1-downloads-api-appservice.azurewebsites.net/Property
        """
        with cls.cursor() as cur:
            cur.execute("SELECT pollutant, definition_url FROM property;")
            return [
                dict(notation=pollutant, id=definition_url)
                for (pollutant, definition_url) in cur
            ]
