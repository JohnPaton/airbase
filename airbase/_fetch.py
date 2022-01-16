from __future__ import annotations

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
