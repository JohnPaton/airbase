import json
import re
from typing import Iterator

import pytest
from responses import Response

from airbase._fetch import fetch_json, fetch_text, fetch_urls

JSON_PAYLOAD = {"payload": "test"}


@pytest.fixture
def echo_url(responses) -> Iterator[str]:
    r = Response(
        method="GET",
        url=re.compile(r"https://echo\.test/.*"),
        json=JSON_PAYLOAD,
    )
    responses.add(r)
    yield "https://echo.test"
    responses.remove(r)


def test_fetch_urls(echo_url: str):
    urls = [
        f"{echo_url}/this/is/a.test",
        f"{echo_url}/this/is/another.test",
    ]
    responses = list(fetch_urls(*urls))
    assert len(responses) == len(urls)
    assert [r.url for r in responses] == urls


def test_fetch_json(echo_url: str):
    url = f"{echo_url}/this/is/test.json"
    json = fetch_json(url)
    assert json == JSON_PAYLOAD


def test_fetch_text(echo_url: str):
    url = f"{echo_url}/this/is/test.txt"
    text = fetch_text(url)
    assert json.loads(text) == JSON_PAYLOAD
