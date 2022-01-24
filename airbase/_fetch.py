from __future__ import annotations

import asyncio
import json
import sys
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
    async def fetch() -> str:
        timeout_ = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_) as session:
            async with session.get(url, ssl=False) as r:
                r.raise_for_status()
                return await r.text(encoding=encoding)

    text = asyncio.run(fetch())
    return text


def fetch_json(
    url: str,
    *,
    timeout: float | None = None,
    encoding: str | None = None,
) -> list[dict[str, str]]:
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
) -> AsyncIterator[str]:
    ...


@overload
def fetcher(
    urls: dict[str, Path],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> AsyncIterator[Path]:
    ...


async def fetcher(
    urls: list[str] | dict[str, Path],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
):

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        @overload
        async def fetch(url: str) -> str:
            ...

        @overload
        async def fetch(url: str, *, path: Path) -> Path:
            ...

        async def fetch(url: str, *, path: Path | None = None):
            async with semaphore:
                async with session.get(url, ssl=False) as r:
                    r.raise_for_status()
                    text = await r.text(encoding=encoding)
                    if path is None:
                        return text
                    async with aiofiles.open(str(path), mode="w") as f:
                        await f.write(text)
                    return path

        jobs: list[Awaitable[str | Path]]
        if isinstance(urls, dict):
            jobs = [fetch(url, path=path) for url, path in urls.items()]
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
                    if not raise_for_status:
                        print(f"Warning: {e}", file=sys.stderr)
                        continue
                    raise


def fetch_unique_lines(
    urls: list[str],
    *,
    encoding: str | None = None,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> set[str]:
    async def fetch() -> set[str]:
        lines = set()
        async for text in fetcher(
            urls,
            encoding=encoding,
            progress=progress,
            raise_for_status=raise_for_status,
            max_concurrent=max_concurrent,
        ):
            lines.update(text.splitlines())
        return lines

    return asyncio.run(fetch())


def fetch_to_file(
    urls: list[str],
    path: Path,
    *,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> None:
    async def fetch() -> None:
        first = True
        async for text in fetcher(
            urls,
            progress=progress,
            raise_for_status=raise_for_status,
            max_concurrent=max_concurrent,
        ):
            if first:
                # keep header line
                async with aiofiles.open(str(path), mode="w") as f:
                    await f.write(text)
                first = False
            else:
                # drop the 1st line
                lines = text.splitlines(keepends=True)[1:]
                async with aiofiles.open(str(path), mode="a") as f:
                    await f.writelines(lines)

    asyncio.run(fetch())


def fetch_to_directory(
    urls: list[str],
    root: Path,
    *,
    skip_existing: bool = True,
    progress: bool = DEFAULT.progress,
    raise_for_status: bool = DEFAULT.raise_for_status,
    max_concurrent: int = DEFAULT.max_concurrent,
) -> None:

    url_paths: dict[str, Path] = {url: root / Path(url).name for url in urls}
    if skip_existing:
        url_paths = {
            url: path for url, path in url_paths.items() if not path.exists()
        }

    async def fetch():
        async for path in fetcher(
            url_paths,
            progress=progress,
            raise_for_status=raise_for_status,
            max_concurrent=max_concurrent,
        ):
            pass

    asyncio.run(fetch())
