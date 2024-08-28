#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from airbase.download_api.download_session import pollutant_id_from_url

CREATE_DB = """
DROP TABLE IF EXISTS city;
CREATE TABLE city (
    country_code TEXT NOT NULL,
    city_name    INTEGER,
    UNIQUE (country_code, city_name)
);


DROP VIEW IF EXISTS countries;
CREATE VIEW countries AS
SELECT DISTINCT
    country_code
FROM
    city
ORDER BY
    country_code;


DROP TABLE IF EXISTS property;
CREATE TABLE property (
    pollutant TEXT NOT NULL,
    pollutant_id INTEGER NOT NULL,
    definition_url TEXT NOT NULL,
    UNIQUE (pollutant, pollutant_id)
);


DROP VIEW IF EXISTS pollutants;
CREATE VIEW pollutants AS
SELECT DISTINCT
    pollutant, pollutant_id
FROM
    property
ORDER BY
    length(pollutant);
"""

INSERT_CITY_JSON = """
INSERT OR REPLACE INTO city (country_code, city_name)
VALUES (:countryCode, :cityName);
"""

INSERT_PROPERTY_JSON = """
INSERT OR IGNORE INTO property (pollutant, pollutant_id, definition_url)
VALUES (:notation, :id, :url);
"""


def main(
    db_path: Path = Path("airbase/summary/summary2.sqlite"),
    data_path: Path = Path("tests/resources"),
):
    city: list[dict] = json.loads(data_path.joinpath("city.json").read_text())
    country: list[dict] = json.loads(
        data_path.joinpath("country.json").read_text()
    )
    missing_counties = set(country["countryCode"] for country in country)
    missing_counties -= set(country["countryCode"] for country in city)
    for country_code in sorted(missing_counties):
        city.append(dict(countryCode=country_code, cityName=None))

    property: list[dict] = json.loads(
        data_path.joinpath("property.json").read_text()
    )
    for poll in property:
        poll.update(url=poll["id"], id=pollutant_id_from_url(poll["id"]))

    with sqlite3.connect(db_path) as db, closing(db.cursor()) as cur:
        cur.executescript(CREATE_DB)
        cur.executemany(INSERT_CITY_JSON, city)
        cur.executemany(INSERT_PROPERTY_JSON, property)


if __name__ == "__main__":
    main()
