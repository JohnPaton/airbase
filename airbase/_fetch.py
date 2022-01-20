from __future__ import annotations

import asyncio
import json
import sys
from typing import AsyncIterator, Iterable, NamedTuple

import aiohttp
from tqdm import tqdm


class TextResponse(NamedTuple):
    url: str
    text: str


def fetch_text(
    url: str, *, timeout: float | None = None, encoding: str | None = None
) -> str:
    async def fetch() -> str:
        timeout_ = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_) as session:
            async with session.get(url, ssl=False) as r:
                r.raise_for_status()
                return await r.text(encoding=encoding)

    text = asyncio.run(fetch())
    return text


def fetch_json(
    url: str, *, timeout: float | None = None, encoding: str | None = None
) -> list[dict]:
    text = fetch_text(url, timeout=timeout, encoding=encoding)
    payload = json.loads(text)
    if isinstance(payload, dict):
        return [payload]
    return payload


async def fetch_all_text(
    urls: Iterable[str],
    progress: bool = False,
    encoding: str | None = None,
    raise_for_status: bool = True,
    max_concurrent: int = 10,
) -> AsyncIterator[TextResponse]:

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch(url: str) -> TextResponse:
            async with semaphore:
                async with session.get(url, ssl=False) as r:
                    r.raise_for_status()
                    text = await r.text(encoding=encoding)
                    return TextResponse(url, text)

        jobs = [fetch(url) for url in urls]
        with tqdm(total=len(jobs), leave=True, disable=not progress) as p_bar:
            for result in asyncio.as_completed(jobs):
                p_bar.update(1)
                try:
                    yield await result
                except asyncio.CancelledError:
                    continue
                except aiohttp.client.ClientResponseError as e:
                    if not raise_for_status:
                        print(f"Warning: {e}", file=sys.stderr)
                        continue
                    raise
