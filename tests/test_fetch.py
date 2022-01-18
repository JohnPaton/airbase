from __future__ import annotations

import pytest

from airbase._fetch import fetch_all_text, fetch_json, fetch_text

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


def test_fetch_json(json_url: str):
    assert fetch_json(json_url) == JSON_PAYLOAD


def test_fetch_text(text_url: str):
    assert fetch_text(text_url) == TEXT_PAYLOAD


@pytest.fixture
def test_urls(response):
    """mock several websites w/text body"""
    urls = {
        "https://echo.test/this_is_a_test": "a.test",
        "https://echo.test/this_is_another_test": "another.test",
    }
    for url, body in urls.items():
        response.get(url=url, body=body)
    yield urls


@pytest.mark.asyncio()
async def test_fetch_all_text(test_urls: dict[str, str]):
    results = []
    async for r in fetch_all_text(test_urls):
        assert r.text == test_urls[r.url]
        results.append(r.url)
    assert sorted(results) == sorted(test_urls)
