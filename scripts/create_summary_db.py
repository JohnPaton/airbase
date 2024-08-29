#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

from airbase.download_api.api_client import CityDict, CountryDict, PropertyDict
from airbase.download_api.download_session import pollutant_id_from_url

BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"


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

INSERT_CITY_JSON = """
INSERT OR REPLACE INTO city (country_code, city_name)
VALUES (:countryCode, :cityName);
"""

INSERT_PROPERTY_JSON = """
INSERT OR IGNORE INTO property (pollutant, pollutant_id, definition_url)
VALUES (:notation, :id, :url);
"""


def main(db_path: Path = Path("airbase/summary/summary.sqlite")):
    country = country_json()
    country_codes = set(country["countryCode"] for country in country)

    city = city_json(*country_codes)
    country_codes -= set(country["countryCode"] for country in city)
    for country_code in sorted(country_codes):
        city.append(dict(countryCode=country_code, cityName=None))  # type:ignore[typeddict-item]

    property = property_json()
    for poll in property:
        poll.update(url=poll["id"], id=pollutant_id_from_url(poll["id"]))  # type:ignore[call-arg]

    with sqlite3.connect(db_path) as db, closing(db.cursor()) as cur:
        cur.executescript(CREATE_DB)
        cur.executemany(INSERT_CITY_JSON, city)
        cur.executemany(INSERT_PROPERTY_JSON, property)


def country_json() -> list[CountryDict]:
    cmd = f"curl -s -X 'GET' '{BASE_URL}/Country' -H 'accept: text/plain'"
    payload = subprocess.check_output(cmd, shell=True, encoding="UTF-8")
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


def city_json(*country_codes: str) -> list[CityDict]:
    cmd = (
        f"curl -s -X 'POST' '{BASE_URL}/City' "
        " -H 'accept: text/plain'"
        " -H 'Content-Type: application/json'"
        f" -d '{json.dumps(sorted(country_codes))}'"
    )
    payload = subprocess.check_output(cmd, shell=True, encoding="UTF-8")
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


def property_json() -> list[PropertyDict]:
    cmd = f"curl -s -X 'GET' '{BASE_URL}/Property'  -H 'accept: text/plain'"
    payload = subprocess.check_output(cmd, shell=True)
    assert payload, "no data"
    return json.loads(payload)  # type:ignore[no-any-return]


if __name__ == "__main__":
    main()
