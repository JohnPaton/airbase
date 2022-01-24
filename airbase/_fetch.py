from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import AsyncIterator, Iterable, NamedTuple

import aiofiles
import aiohttp
from tqdm import tqdm

DEFAULT = SimpleNamespace(
    progress=False,
    raise_for_status=True,
    max_concurrent=10,
)


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
) -> list[dict[str, str]]:
    text = fetch_text(url, timeout=timeout, encoding=encoding)
    payload: dict[str, str] | list[dict[str, str]]
    payload = json.loads(text)
    if isinstance(payload, dict):
        return [payload]
    return payload


async def fetch_all_text(
    urls: Iterable[str],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
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


def fetch_unique_lines(
    urls: Iterable[str],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> set[str]:
    async def fetch() -> set[str]:
        lines = set()
        async for r in fetch_all_text(
            urls,
            encoding=encoding,
            progress=progress,
            raise_for_status=raise_for_status,
            max_concurrent=max_concurrent,
        ):
            lines.update(r.text.splitlines())
        return lines

    return asyncio.run(fetch())


async def fetch_to_path(
    url_paths: dict[str, Path],
    *,
    append: bool = False,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> None:

    async for r in fetch_all_text(
        url_paths,
        progress=progress,
        raise_for_status=raise_for_status,
        max_concurrent=max_concurrent,
    ):
        path = url_paths[r.url]
        if append and path.exists():
            # drop the 1st line
            lines = r.text.splitlines(keepends=True)[1:]
            async with aiofiles.open(str(path), mode="a") as f:
                await f.writelines(lines)
        else:
            # keep header line
            async with aiofiles.open(str(path), mode="w") as f:
                await f.write(r.text)
