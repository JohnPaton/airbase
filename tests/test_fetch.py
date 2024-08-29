from __future__ import annotations

import itertools
from pathlib import Path

import aiohttp
import pytest

from airbase.fetch import (
    fetch_json,
    fetch_text,
    fetch_to_file,
)
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


def test_fetch_to_file(tmp_path: Path, csv_urls: dict[str, str]):
    path = tmp_path / "single_file.test"
    assert not path.exists()

    fetch_to_file(list(csv_urls), path)
    assert path.exists()

    # drop the header and compare data rows
    def rows(text: str) -> list[str]:
        return text.splitlines()[1:]

    data_on_file = rows(path.read_text())
    data_rows = list(
        itertools.chain.from_iterable(rows(text) for text in csv_urls.values())
    )
    assert sorted(data_on_file) == sorted(data_rows)


def test_fetchtest_fetch_to_file_error(tmp_path: Path, bad_request_url: str):
    path = tmp_path / "single_file.test"
    with pytest.raises(aiohttp.ClientResponseError):
        fetch_to_file([bad_request_url], path)
    assert not path.exists()


def test_fetchtest_fetch_to_file_warning(tmp_path: Path, bad_request_url: str):
    path = tmp_path / "single_file.test"
    with pytest.warns(RuntimeWarning):
        fetch_to_file([bad_request_url], path, raise_for_status=False)
    assert not path.exists()
