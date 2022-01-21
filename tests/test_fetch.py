from __future__ import annotations

import asyncio
import itertools
from pathlib import Path

import pytest

from airbase._fetch import fetch_all_text, fetch_json, fetch_text, fetch_to_path

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
    return urls


@pytest.mark.asyncio()
async def test_fetch_all_text(test_urls: dict[str, str]):
    results = []
    async for r in fetch_all_text(test_urls):
        assert r.text == test_urls[r.url]
        results.append(r.url)
    assert sorted(results) == sorted(test_urls)


@pytest.fixture
def url_paths(tmp_path: Path, test_urls: dict[str, str]):
    """mock several websites w/text body"""
    return {url: tmp_path / path for url, path in test_urls.items()}


def test_fetch_to_path(url_paths: dict[str, Path]):
    assert not any(path.exists() for path in url_paths.values())
    asyncio.run(fetch_to_path(url_paths))
    assert all(path.exists() for path in url_paths.values())


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


def test_fetch_to_path_append(tmp_path: Path, csv_urls: dict[str, str]):
    path = tmp_path / "append.test"
    assert not path.exists()

    url_paths = dict.fromkeys(csv_urls, path)
    asyncio.run(fetch_to_path(url_paths, append=True))
    assert path.exists()

    # drop the header and compare data rows
    rows = lambda text: text.splitlines()[1:]
    data_on_file = rows(path.read_text())
    data_rows = list(
        itertools.chain.from_iterable(rows(text) for text in csv_urls.values())
    )
    assert sorted(data_on_file) == sorted(data_rows)
