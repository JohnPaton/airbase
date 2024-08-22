from airbase.download_api import COUNTRY_CODES, countries


def test_countries():
    assert set(countries()) == set(COUNTRY_CODES)
