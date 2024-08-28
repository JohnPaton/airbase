#!/usr/bin/env python3

import sqlite3
from contextlib import closing
from pathlib import Path

from airbase.fetch import fetch_json
from airbase.resources import E1A_SUMMARY_URL

CREATE_DB = """
DROP TABLE IF EXISTS summary;
CREATE TABLE summary (
    country_code TEXT NOT NULL,
    pollutant TEXT NOT NULL,
    pollutant_id INTEGER NOT NULL,
    UNIQUE (country_code, pollutant, pollutant_id)
);


DROP VIEW IF EXISTS countries;
CREATE VIEW countries AS
SELECT DISTINCT
    country_code
FROM
    summary
ORDER BY
    country_code;


DROP VIEW IF EXISTS pollutants;
CREATE VIEW pollutants AS
SELECT DISTINCT
    pollutant, pollutant_id
FROM
    summary
ORDER BY
    length(pollutant);
"""

INSERT_JSON = """
INSERT OR IGNORE INTO summary (country_code, pollutant, pollutant_id)
VALUES (:ct, :pl, :shortpl);
"""


def main(db_path: Path = Path("airbase/summary/summary.sqlite")):
    summary = fetch_json(E1A_SUMMARY_URL)
    with sqlite3.connect(db_path) as db, closing(db.cursor()) as cur:
        cur.executescript(CREATE_DB)
        cur.executemany(INSERT_JSON, summary)


if __name__ == "__main__":
    main()
