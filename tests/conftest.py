import re

import pytest
from responses import Response

import airbase
from tests.resources import (
    CSV_LINKS_RESPONSE_TEXT,
    CSV_RESPONSE,
    METADATA_RESPONSE,
    SUMMARY,
)


@pytest.fixture()
def summary_response(responses):
    r = Response(
        method="GET",
        url=airbase.resources.E1A_SUMMARY_URL,
        json=SUMMARY,
    )
    responses.add(r)
    yield r
    responses.remove(r)


@pytest.fixture()
def csv_links_response(responses):
    r = Response(
        method="GET",
        url=(
            "http://fme.discomap.eea.europa.eu/fmedatastreaming/"
            "AirQualityDownload/AQData_Extract.fmw"
        ),
        body=CSV_LINKS_RESPONSE_TEXT,
        # match_querystring=False, # deprecated option
    )
    responses.add(r)
    yield r
    responses.remove(r)


@pytest.fixture()
def csv_response(responses):
    r = Response(
        method="GET",
        url=re.compile(
            r"https://ereporting\.blob\.core\.windows\.net/downloadservice/.*"
        ),
        body=CSV_RESPONSE,
    )
    responses.add(r)
    yield r
    responses.remove(r)


@pytest.fixture()
def metadata_response(responses):
    r = Response(
        method="GET",
        url=airbase.resources.METADATA_URL,
        body=METADATA_RESPONSE,
    )
    responses.add(r)
    yield r
    responses.remove(r)
