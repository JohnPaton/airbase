import re

import pytest
from aioresponses import aioresponses

from airbase.resources import METADATA_URL
from airbase.summary import DB
from tests import resources


@pytest.fixture
def response():
    """aioresponses as a fixture"""
    with aioresponses() as mocker:
        yield mocker


@pytest.fixture
def mock_api(response: aioresponses):
    """mock responses from Download API entry points"""
    response.get(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/Country",
        payload=DB.country_json(),
    )
    response.get(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/Property",
        payload=DB.property_json(),
    )
    response.post(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/City",
        payload=DB.city_json(),
    )
    response.post(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/DownloadSummary",
        body=resources.JSON_DOWNLOAD_SUMMARY_RESPONSE,
        repeat=True,
    )
    response.post(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls",
        body=resources.CSV_PARQUET_URLS_RESPONSE,
        repeat=True,
    )
    response.get(
        re.compile(r"https://.*/../.*\.parquet"),  # any parquet file
        body=b"",
        repeat=True,
    )


@pytest.fixture()
def metadata_response(response: aioresponses):
    """mock response from metadata url"""
    response.get(
        METADATA_URL,
        body=resources.METADATA_RESPONSE,
    )
