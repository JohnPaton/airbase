import re

import pytest
import responses as rsps  # avoid name collision with fixture

import airbase


@pytest.fixture()
def summary_response(responses):
    from .resources import SUMMARY

    r = rsps.Response(
        method="GET", url=airbase.resources.E1A_SUMMARY_URL, json=SUMMARY
    )
    responses.add(r)
    yield r
    responses.remove(r)


@pytest.fixture()
def csv_links_response(responses):
    from .resources import CSV_LINKS_RESPONSE_TEXT

    r = rsps.Response(
        method="GET",
        url="http://fme.discomap.eea.europa.eu/fmedatastreaming/"
        "AirQualityDownload/AQData_Extract.fmw",
        body=CSV_LINKS_RESPONSE_TEXT,
        match_querystring=False,
    )
    responses.add(r)
    yield r
    responses.remove(r)


@pytest.fixture()
def csv_response(responses):
    from .resources import CSV_RESPONSE

    r = rsps.Response(
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
    from .resources import METADATA_RESPONSE

    r = rsps.Response(
        method="GET", url=airbase.resources.METADATA_URL, body=METADATA_RESPONSE
    )
    responses.add(r)
    yield r
    responses.remove(r)


@pytest.fixture()
def all_responses(
    summary_response, csv_links_response, csv_response, metadata_response
):
    return
