from __future__ import annotations

import aiohttp
import pytest

from airbase.fetch import fetch_json, fetch_text
from tests.resources import CSV_LINKS_RESPONSE_TEXT

JSON_PAYLOAD = [{"payload": "test"}]
TEXT_PAYLOAD = "this is a test"


@pytest.fixture
def json_url(response):
    """mock website w/json payload"""
    url = "https://echo.test/json"
    response.get(url=url, payload=JSON_PAYLOAD)
    yield url


@pytest.fixture
def text_url(response):
    """mock website w/text body"""
    url = "https://echo.test/text"
    response.get(url=url, body=TEXT_PAYLOAD)
    yield url


@pytest.fixture
def bad_request_url(response):
    """mock website w/json payload"""
    url = "https:/echo.test/bad_request"
    response.get(url=url, status=400)
    yield url


def test_fetch_json(json_url: str):
    assert fetch_json(json_url) == JSON_PAYLOAD


def test_fetch_json_error(bad_request_url: str):
    with pytest.raises(aiohttp.ClientResponseError):
        fetch_json(bad_request_url)


def test_fetch_text(text_url: str):
    assert fetch_text(text_url) == TEXT_PAYLOAD


def test_fetch_text_error(bad_request_url: str):
    with pytest.raises(aiohttp.ClientResponseError):
        fetch_text(bad_request_url)


@pytest.fixture
def csv_links_url(response):
    """mock several websites w/csv_links response"""
    urls = [
        "https://echo.test/csv_links",
        "https://echo.test/more_csv_links",
    ]
    for url in urls:
        response.get(url=url, body=CSV_LINKS_RESPONSE_TEXT)
    return urls


@pytest.fixture
def csv_urls(response):
    """mock several websites w/text body"""
    urls = {
        "https://echo.test/this_is_a_test": "#header\na,csv,test,file\n",
        "https://echo.test/this_is_another_test": "#header\nanother,csv,test,file\n",
    }
    for url, body in urls.items():
        response.get(url=url, body=body)
    return urls
