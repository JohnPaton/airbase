from airbase.fetch import fetch_json
from airbase.resources import E1A_SUMMARY_URL
from airbase.summary.db import summary


def test_summary():
    assert summary() == fetch_json(E1A_SUMMARY_URL)
