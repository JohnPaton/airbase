import re

import pytest
from aioresponses import aioresponses

import airbase
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
        repeat=False,
    )
    response.get(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/Property",
        payload=DB.property_json(),
        repeat=False,
    )
    response.post(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/City",
        payload=DB.city_json(),
        repeat=False,
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
def csv_links_response(response: aioresponses):
    """mock response from station data links url"""
    response.get(
        re.compile(
            r"http://fme\.discomap\.eea\.europa\.eu/fmedatastreaming/"
            r"AirQualityDownload/AQData_Extract\.fmw.*"
        ),
        body=resources.CSV_LINKS_RESPONSE_TEXT,
        repeat=True,
    )


@pytest.fixture()
def csv_response(response: aioresponses):
    """mock response from station data url"""
    response.get(
        re.compile(
            r"https://ereporting\.blob\.core\.windows\.net/downloadservice/.*"
        ),
        body=resources.CSV_RESPONSE,
        repeat=True,
    )


@pytest.fixture()
def metadata_response(response: aioresponses):
    """mock response from metadata url"""
    response.get(
        airbase.resources.METADATA_URL,
        body=resources.METADATA_RESPONSE,
    )
