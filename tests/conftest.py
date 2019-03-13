import pytest


@pytest.fixture(scope="session")
def summary():
    from .resources import SUMMARY
    return SUMMARY


@pytest.fixture(scope="session")
def csv_links_response_text():
    from .resources import CSV_LINKS_RESPONSE_TEXT
    return CSV_LINKS_RESPONSE_TEXT
