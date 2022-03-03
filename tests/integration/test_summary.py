from __future__ import annotations

import pytest

from airbase.fetch import fetch_json
from airbase.resources import E1A_SUMMARY_URL
from airbase.summary import DB


@pytest.fixture(scope="module")
def db_dump() -> list[dict[str, str]]:
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT
                country_code,
                pollutant,
                cast(pollutant_id AS TEXT)
            FROM
                summary;
            """
        )
        return [dict(ct=ct, pl=pl, shortpl=shortpl) for ct, pl, shortpl in cur]


def test_summary(db_dump: list[dict[str, str]]):
    assert db_dump == fetch_json(E1A_SUMMARY_URL)
