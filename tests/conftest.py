from __future__ import annotations

import re

import pytest
from aioresponses import aioresponses

from airbase.summary import DB
from tests import resources


@pytest.fixture
def response():
    """aioresponses as a fixture"""
    with aioresponses() as mocker:
        yield mocker


@pytest.fixture
def mock_parquet_api(response: aioresponses):
    """mock responses from Parquet downloads API"""
    response.get(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/Country",
        payload=DB.country_json(),
    )
    response.get(
        "https://eeadmz1-downloads-api-appservice.azurewebsites.net/Pollutant",
        payload=DB.pollutant_json(),
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
        "https://discomap.eea.europa.eu/App/AQViewer/download?fqn=Airquality_Dissem.b2g.measurements&f=csv",
        body=resources.ZIP_CSV_METADATA_RESPONSE,
        repeat=False,
    )
    response.get(
        re.compile(r"https://.*/../.*\.parquet"),  # any parquet file
        body=b"",
        repeat=True,
    )
