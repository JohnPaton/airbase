import re
from typing import Iterator

import pytest
from responses import Response

from airbase._fetch import fetch_urls


@pytest.fixture
def echo_url(responses) -> Iterator[str]:
    r = Response(method="GET", url=re.compile(r"https://echo\.test/.*"))
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
