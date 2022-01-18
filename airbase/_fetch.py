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


async def _fetch_url(
    url: str, *, session: aiohttp.ClientSession, encoding: str | None = None
) -> TextResponse:
    async with session.get(url, ssl=False) as r:
        r.raise_for_status()
        text = await r.text(encoding=encoding)
        return TextResponse(url, text)


async def _fetch_text(
    url: str,
    encoding: str | None = None,
    timeout: float | None = None,
) -> str:

    _timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=_timeout) as session:
        async with session.get(url, ssl=False) as r:
            r.raise_for_status()
            return await r.text(encoding=encoding)


def fetch_text(
    url: str, *, timeout: float | None = None, encoding: str | None = None
) -> str:
    text = asyncio.run(_fetch_text(url, encoding=encoding, timeout=timeout))
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
) -> AsyncIterator[TextResponse]:

    async with aiohttp.ClientSession() as session:
        tasks = {
            asyncio.create_task(
                _fetch_url(url, session=session, encoding=encoding)
            )
            for url in urls
        }
        with tqdm(total=len(tasks), leave=True, disable=not progress) as p_bar:
            while tasks:
                done, tasks = await asyncio.wait(
                    tasks, timeout=None, return_when=asyncio.FIRST_COMPLETED
                )

                for future in done:
                    try:
                        yield await future
                    except asyncio.CancelledError:
                        continue
                    except aiohttp.client.ClientResponseError as e:
                        if not raise_for_status:
                            print(f"Warning: {e}", file=sys.stderr)
                            continue
                        raise
                    finally:
                        p_bar.update(1)
