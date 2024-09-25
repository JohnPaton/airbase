#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

from airbase.parquet_api.session import pollutant_id_from_url
from airbase.parquet_api.types import (
    CityJSON,
    CountryJSON,
    PollutantJSON,
)

BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"


CREATE_DB = """
DROP TABLE IF EXISTS country;
CREATE TABLE country(
    country_code TEXT NOT NULL,
    country_name TEXT NOT NULL,
    UNIQUE (country_code, country_name)
);


DROP VIEW IF EXISTS countries;
CREATE VIEW countries AS
SELECT DISTINCT
    country_code
FROM
    country
ORDER BY
    country_code;


DROP TABLE IF EXISTS city;
CREATE TABLE city (
    country_code TEXT NOT NULL,
    city_name    TEXT NOT NULL,
    UNIQUE (country_code, city_name)
);


DROP TABLE IF EXISTS pollutant;
CREATE TABLE pollutant (
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
    pollutant
ORDER BY
    length(pollutant), pollutant_id;


DROP VIEW IF EXISTS pollutant_ids;
CREATE VIEW pollutant_ids AS
SELECT DISTINCT
    pollutant, GROUP_CONCAT(pollutant_id) AS ids
FROM
    pollutant
GROUP BY
    pollutant
ORDER BY
    length(pollutant), pollutant_id;
"""

INSERT_COUNTRY_JSON = """
INSERT OR IGNORE INTO country (country_code, country_name)
VALUES (:countryCode, :countryName);
"""

INSERT_CITY_JSON = """
INSERT OR IGNORE INTO city (country_code, city_name)
VALUES (:countryCode, :cityName);
"""

INSERT_PROPERTY_JSON = """
INSERT OR IGNORE INTO pollutant (pollutant, pollutant_id, definition_url)
VALUES (:notation, :id, :url);
"""


def main(db_path: Path = Path("airbase/summary/summary.sqlite")):
    with sqlite3.connect(db_path) as db, closing(db.cursor()) as cur:
        # recreate tables and views
        cur.executescript(CREATE_DB)

        # populate city table
        for country in country_json():
            cur.execute(INSERT_COUNTRY_JSON, country)
            cur.executemany(INSERT_CITY_JSON, city_json(country["countryCode"]))

        # populate pollutant table
        pollutant = pollutant_json()
        for poll in pollutant:
            poll.update(url=poll["id"], id=pollutant_id_from_url(poll["id"]))  # type:ignore[call-arg]
        cur.executemany(INSERT_PROPERTY_JSON, pollutant)


def country_json() -> CountryJSON:
    cmd = f"curl -s -X 'GET' '{BASE_URL}/Country' -H 'accept: text/plain'"
    payload = subprocess.check_output(cmd, shell=True, encoding="UTF-8")
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


def city_json(*country_codes: str) -> CityJSON:
    cmd = (
        f"curl -s -X 'POST' '{BASE_URL}/City' "
        " -H 'accept: text/plain'"
        " -H 'Content-Type: application/json'"
        f" -d '{json.dumps(sorted(country_codes))}'"
    )
    payload = subprocess.check_output(cmd, shell=True, encoding="UTF-8")
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


def pollutant_json() -> PollutantJSON:
    cmd = f"curl -s -X 'GET' '{BASE_URL}/Pollutant'  -H 'accept: text/plain'"
    payload = subprocess.check_output(cmd, shell=True)
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


if __name__ == "__main__":
    main()
