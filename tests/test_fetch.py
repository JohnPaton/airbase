from __future__ import annotations

import aiohttp
import pytest

from airbase.fetch import fetch_text

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


def test_fetch_text(text_url: str):
    assert fetch_text(text_url) == TEXT_PAYLOAD


def test_fetch_text_error(bad_request_url: str):
    with pytest.raises(aiohttp.ClientResponseError):
        fetch_text(bad_request_url)
