"""Helper functions encapsulating async HTTP request and file IO"""

from __future__ import annotations

import asyncio

import aiohttp


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
