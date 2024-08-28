from __future__ import annotations

import sqlite3
import sys
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Iterator

if sys.version_info >= (3, 11):  # pragma: no cover
    from importlib import resources
else:  # pragma: no cover
    import importlib_resources as resources


def summary() -> Path:
    source = resources.files(__package__) / "summary.sqlite"
    path: Path
    with resources.as_file(source) as path:
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
