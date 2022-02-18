from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import Iterator


class Summary:
    """
    In memory DB containing the available country and pollutants
    """

    db = sqlite3.connect(":memory:")

    with closing(db.cursor()) as cur:
        cur.executescript(
            """            
            CREATE TABLE summary (
                country_code TEXT NOT NULL,
                pollutant TEXT NOT NULL,
                pollutant_id INTEGER NOT NULL,
                UNIQUE (country_code, pollutant, pollutant_id)
            );

            
            CREATE VIEW countries AS
            SELECT DISTINCT
                country_code
            FROM
                summary
            ORDER BY
                country_code;


            CREATE VIEW pollutants AS
            SELECT DISTINCT
                pollutant, pollutant_id
            FROM
                summary
            ORDER BY
                length(pollutant);


            CREATE VIEW pollutants_per_country AS
            SELECT
               country_code,
               GROUP_CONCAT(pollutant),
               GROUP_CONCAT(pollutant_id)
            FROM
               summary
            GROUP BY
               country_code
            ORDER BY
               country_code;
            """
        )

    def __init__(self, summary: list[dict[str, str]]) -> None:
        """
        Ingest available countries and pollutants

        :param summary: The E1a summary.
        """

        self.__inset(summary)

    @classmethod
    def __inset(cls, summary: list[dict[str, str]]) -> None:

        insert = """
        INSERT OR IGNORE INTO summary (country_code, pollutant, pollutant_id)
        VALUES (:ct, :pl, :shortpl);
        """
        with closing(cls.db.cursor()) as cur:
            cur.executemany(insert, summary)

    @classmethod
    def countries(self) -> list[str]:
        """
        Get the list of unique countries from the summary.

        :return: list of available country codes
        """

        with closing(self.db.cursor()) as cur:
            cur.execute("SELECT * FROM countries;")
            return list(row[0] for row in cur.fetchall())

    @classmethod
    def pollutants(self) -> dict[str, str]:
        """
        Get the list of unique pollutants from the summary.

        :param summary: The E1a summary.

        :return: The available pollutants, as a dictionary with
        with name as keys with name as values, e.g. {"NO": "38", ...}
        """

        with closing(self.db.cursor()) as cur:
            cur.execute("SELECT * FROM pollutants;")
            return dict(cur.fetchall())

    @classmethod
    def search_pollutant(
        self, query: str, *, limit: int | None = None
    ) -> dict[str, int]:
        """
        Search for a pollutant's ID number based on its name.

        :param query: The pollutant to search for.
        :param limit: (optional) Max number of results.

        :return: The best pollutant matches, as a dictionary with
        with name as keys with name as values, e.g. {"NO": 38, ...}
        """

        with closing(self.db.cursor()) as cur:
            cur.execute(
                f"""
                SELECT * FROM pollutants
                WHERE pollutant LIKE '%{query}%'
                {f"LIMIT {limit}" if limit else ""};
                """
            )
            return dict(cur.fetchall())

    @classmethod
    def pollutants_per_country(self) -> dict[str, dict[str, int]]:
        """
        Get the available pollutants per country from the summary.

        :return: All available pollutants per country, as a dictionary with
        with country code as keys and a dictionary of pollutant/ids
        (e.g. {"NO": "38", ...}) as values.
        """

        with closing(self.db.cursor()) as cur:
            cur.execute("SELECT * FROM pollutants_per_country;")
            return {
                ct: dict(zip(pollutants.split(","), map(int, ids.split(","))))
                for ct, pollutants, ids in cur
            }
