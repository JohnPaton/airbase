from __future__ import annotations

import asyncio
from typing import Literal

import aiohttp

API_SERVICE_ROOT = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
COUNTRY_CODES = set(
    """
    AD AL AT BA BE BG CH CY CZ DE DK EE ES FI FR GB GI GR HR HU
    IE IS IT LI LT LU LV ME MK MT NL NO PL PT RO RS SE SI SK TR
    XK
    """.split()
)


async def get_json_from_api(
    entry_point: Literal["Country"],
    *,
    timeout: float | None = None,
    encoding: str | None = None,
) -> list[dict[str, str]]:
    """
    get request to an specific Download API entry point and return decoded JSON

    :param entry_point: Download API entry point
    :param timeout: maximum time to complete request (seconds)
    :param encoding: text encoding used for decoding the response's body

    :return: decoded JSON from response's body
    """

    timeout_ = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_) as session:
        async with session.get(
            f"{API_SERVICE_ROOT}/{entry_point}", ssl=False
        ) as r:
            r.raise_for_status()
            payload: list[dict[str, str]] = await r.json(encoding=encoding)
            return payload


def countries() -> list[str]:
    """request country codes from API"""
    payload = asyncio.run(get_json_from_api("Country"))
    return [country["countryCode"] for country in payload]
