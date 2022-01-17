from __future__ import annotations

import sys
from typing import Iterator

from requests import Response, Session
from tqdm import tqdm


def fetch_urls(
    *urls: str, progress: bool = False, timeout: float | None = None
) -> Iterator[Response]:
    """request urls one by one"""
    with Session() as session:
        for url in tqdm(urls, leave=True, disable=not progress):
            yield session.get(url, timeout=timeout)


def fetch_json(url: str, *, timeout: float | None = None) -> dict:
    r, *_ = fetch_urls(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_text(url: str, *, encoding: str | None = None) -> str:
    (text, url), *_ = fetch_all_text(url, encoding=encoding)
    return text


def fetch_all_text(
    *url: str,
    progress: bool = False,
    encoding: str | None = None,
    raise_for_status: bool = True,
) -> Iterator[tuple[str, str]]:
    for r in fetch_urls(*url, progress=progress):
        try:
            r.raise_for_status()
        except Exception as e:
            if not raise_for_status:
                print(f"Warning: {e}", file=sys.stderr)
                continue
            raise
        if encoding is not None:
            r.encoding = encoding
        yield r.text, r.url
