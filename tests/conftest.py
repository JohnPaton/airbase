import re

import pytest
from aioresponses import aioresponses

import airbase
from tests import resources


@pytest.fixture
def response():
    """aioresponses as a fixture"""
    with aioresponses() as mocker:
        yield mocker


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
