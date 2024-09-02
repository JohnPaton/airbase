#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

from airbase.download_api.abstract_api_client import (
    CityJSON,
    CountryJSON,
    PropertyJSON,
)
from airbase.download_api.download_session import pollutant_id_from_url

BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"


CREATE_DB = """
DROP TABLE IF EXISTS city;
CREATE TABLE city (
    country_code TEXT NOT NULL,
    city_name    TEXT,
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
    length(pollutant), pollutant_id;


DROP VIEW IF EXISTS pollutant_ids;
CREATE VIEW pollutant_ids AS
SELECT DISTINCT
    pollutant, GROUP_CONCAT(pollutant_id) AS ids
FROM
    property
GROUP BY
    pollutant
ORDER BY
    length(pollutant), pollutant_id;
"""

INSERT_COUNTRY_JSON = """
INSERT OR IGNORE INTO city (country_code, city_name)
VALUES (:countryCode, NULL);
"""

INSERT_CITY_JSON = """
INSERT OR IGNORE INTO city (country_code, city_name)
VALUES (:countryCode, :cityName);
"""

INSERT_PROPERTY_JSON = """
INSERT OR IGNORE INTO property (pollutant, pollutant_id, definition_url)
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

        # populate property table
        property = property_json()
        for poll in property:
            poll.update(url=poll["id"], id=pollutant_id_from_url(poll["id"]))  # type:ignore[call-arg]
        cur.executemany(INSERT_PROPERTY_JSON, property)


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


def property_json() -> PropertyJSON:
    cmd = f"curl -s -X 'GET' '{BASE_URL}/Property'  -H 'accept: text/plain'"
    payload = subprocess.check_output(cmd, shell=True)
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


if __name__ == "__main__":
    main()
