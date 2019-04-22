import pytest
import responses

import airbase


@pytest.fixture(scope="session")
def summary_response():
    from .resources import SUMMARY

    return responses.Response(
        method="GET", url=airbase.resources.E1A_SUMMARY_URL, json=SUMMARY
    )


@pytest.fixture(scope="session")
def csv_links_response():
    from .resources import CSV_LINKS_RESPONSE_TEXT

    return responses.Response(
        method="GET",
        url="http://fme.discomap.eea.europa.eu/fmedatastreaming/"
        "AirQualityDownload/AQData_Extract.fmw CSV_LINKS_RESPONSE_TEXT",
        body=CSV_LINKS_RESPONSE_TEXT,
        match_querystring=False,
    )


@pytest.fixture(scope="session")
def csv_response():
    from .resources import CSV_RESPONSE

    return responses.Response(
        method="GET",
        url="https://ereporting.blob.core.windows.net/downloadservice/.*",
        body=CSV_RESPONSE,
    )


@pytest.fixture(scope="session")
def metadata_response():
    from .resources import METADATA_RESPONSE

    return responses.Response(
        method="GET", url=airbase.resources.METADATA_URL, body=METADATA_RESPONSE
    )


@pytest.fixture
def mocked_client(summary_response):
    responses.add(summary_response)
    client = airbase.AirbaseClient(connect=True)
    return client
