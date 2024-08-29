"""Helper functions encapsulating async HTTP request and file IO"""

from __future__ import annotations

import asyncio
import json
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import AsyncIterator, Awaitable, overload

import aiofiles
import aiohttp
from tqdm import tqdm

DEFAULT = SimpleNamespace(
    progress=False,
    raise_for_status=True,
    max_concurrent=10,
)


def fetch_text(
    url: str,
    *,
    timeout: float | None = None,
    encoding: str | None = None,
) -> str:
    """Request url and read response’s body

    :param url: requested url
    :param timeout: maximum time to complete request (seconds)
    :param encoding: text encoding used for decoding the response's body

    :return: decoded text from response's body
    """

    async def fetch() -> str:
        timeout_ = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_) as session:
            async with session.get(url, ssl=False) as r:
                r.raise_for_status()
                text: str = await r.text(encoding=encoding)
                return text

    text = asyncio.run(fetch())
    return text


def fetch_json(
    url: str,
    *,
    timeout: float | None = None,
    encoding: str | None = None,
) -> list[dict[str, str]]:
    """Request url and read response’s body as JSON

    :param url: requested url
    :param timeout: maximum time to complete request (seconds)
    :param encoding: text encoding used for decoding the response's body

    :return: decoded text from response's body as JSON
    """
    text = fetch_text(url, timeout=timeout, encoding=encoding)
    payload: dict[str, str] | list[dict[str, str]]
    payload = json.loads(text)
    if isinstance(payload, dict):
        return [payload]
    return payload


@overload
def fetcher(
    urls: list[str],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> AsyncIterator[str]:  # pragma: no cover
    ...


@overload
def fetcher(
    urls: dict[str, Path],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> AsyncIterator[Path]:  # pragma: no cover
    ...


async def fetcher(
    urls: list[str] | dict[str, Path],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> AsyncIterator[str | Path]:
    """Request multiple urls and write request text into individual paths
    it a `dict[url, path]` is provided, or return the decoded text from each request
    if only a `list[url]` is provided.

    :param urls: requested urls
    :param encoding: text encoding used for decoding each response's body
    :param progress: show progress bar
    :param raise_for_status: Raise exceptions if download links
        return "bad" HTTP status codes. If False,
        a :py:func:`warnings.warn` will be issued instead.
    :param max_concurrent: maximum concurrent requests

    :return: url text or path to downloaded text, one by one as the requests are completed
    """

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch(url: str) -> str:
            """retrieve text, nothing more"""
            async with semaphore:
                async with session.get(url, ssl=False) as r:
                    r.raise_for_status()
                    text: str = await r.text(encoding=encoding)
                    return text

        async def download(url: str, path: Path) -> Path:
            """retrieve text and write into path"""
            async with semaphore:
                async with session.get(url, ssl=False) as r:
                    r.raise_for_status()
                    text: str = await r.text(encoding=encoding)
                async with aiofiles.open(str(path), mode="w") as f:
                    await f.write(text)
                return path

        jobs: list[Awaitable[str | Path]]
        if isinstance(urls, dict):
            jobs = [download(url, path) for url, path in urls.items()]
        else:
            jobs = [fetch(url) for url in urls]
        with tqdm(total=len(jobs), leave=True, disable=not progress) as p_bar:
            for result in asyncio.as_completed(jobs):
                p_bar.update(1)
                try:
                    yield await result
                except asyncio.CancelledError:
                    continue
                except aiohttp.ClientResponseError as e:
                    if raise_for_status:
                        raise
                    warnings.warn(str(e), category=RuntimeWarning)
