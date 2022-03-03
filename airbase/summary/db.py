from __future__ import annotations

import sqlite3
from collections import defaultdict
from contextlib import closing, contextmanager
from importlib import resources
from pathlib import Path
from typing import Iterator


def summary() -> Path:
    with resources.path(__package__, "summary.sqlite") as path:
        return path


class DB:
    """
    In DB containing the available country and pollutants
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
        Get the list of unique countries from the summary.

        :return: list of available country codes
        """

        with cls.cursor() as cur:
            cur.execute("SELECT country_code FROM countries;")
            return list(row[0] for row in cur.fetchall())

    @classmethod
    def pollutants(cls) -> dict[str, str]:
        """
        Get the list of unique pollutants from the summary.

        :param summary: The E1a summary.

        :return: The available pollutants, as a dictionary with
        with name as keys with name as values, e.g. {"NO": "38", ...}
        """

        with cls.cursor() as cur:
            cur.execute("SELECT pollutant, pollutant_id FROM pollutants;")
            return dict(cur.fetchall())

    @classmethod
    def search_pollutant(
        cls, query: str, *, limit: int | None = None
    ) -> dict[str, int]:
        """
        Search for a pollutant's ID number based on its name.

        :param query: The pollutant to search for.
        :param limit: (optional) Max number of results.

        :return: The best pollutant matches, as a dictionary with
        with name as keys with name as values, e.g. {"NO": 38, ...}
        """

        with cls.cursor() as cur:
            cur.execute(
                f"""
                SELECT pollutant, pollutant_id FROM pollutants
                WHERE pollutant LIKE '%{query}%'
                {f"LIMIT {limit}" if limit else ""};
                """
            )
            return dict(cur.fetchall())

    @classmethod
    def pollutants_per_country(cls) -> dict[str, dict[str, int]]:
        """
        Get the available pollutants per country from the summary.

        :return: All available pollutants per country, as a dictionary with
        with country code as keys and a dictionary of pollutant/ids
        (e.g. {"NO": 38, ...}) as values.
        """

        with cls.cursor() as cur:
            cur.execute(
                "SELECT country_code, pollutant, pollutant_id FROM summary"
            )
            output: dict[str, dict[str, int]] = defaultdict(dict)
            for country_code, pollutant, pollutant_id in cur:
                output[country_code][pollutant] = pollutant_id
            return dict(output)
