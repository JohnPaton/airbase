from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Literal
from warnings import warn

import aiohttp

API_SERVICE_ROOT = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
COUNTRY_CODES = set(
    """
    AD AL AT BA BE BG CH CY CZ DE DK EE ES FI FR GB GI GR HR HU
    IE IS IT LI LT LU LV ME MK MT NL NO PL PT RO RS SE SI SK TR
    XK
    """.split()
)


async def json_from_get_api(
    entry_point: Literal["Country", "Property"],
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


async def json_from_post_api(
    entry_point: Literal["City"],
    data: tuple[str, ...] | list[str],
    *,
    timeout: float | None = None,
    encoding: str | None = None,
) -> list[dict[str, str]]:
    """
    post request to an specific Download API entry point and return decoded JSON

    :param entry_point: Download API entry point
    :param data:
    :param timeout: maximum time to complete request (seconds)
    :param encoding: text encoding used for decoding the response's body

    :return: decoded JSON from response's body
    """

    timeout_ = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_) as session:
        async with session.post(
            f"{API_SERVICE_ROOT}/{entry_point}", json=data, ssl=False
        ) as r:
            r.raise_for_status()
            payload: list[dict[str, str]] = await r.json(encoding=encoding)
            return payload


def countries() -> list[str]:
    """request country codes from API"""
    payload = asyncio.run(json_from_get_api("Country"))
    return [country["countryCode"] for country in payload]


def pollutant_id_from_url(url: str) -> int:
    """
    numeric pollutant id from urls like
        http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1
        http://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/44/view
    """
    if url.endswith("view"):
        return int(url.split("/")[-2])
    return int(url.split("/")[-1])


def pollutants() -> defaultdict[str, set[int]]:
    """requests pollutants id and notation from API"""
    payload = asyncio.run(json_from_get_api("Property"))
    ids: defaultdict[str, set[int]] = defaultdict(set)
    for poll in payload:
        key, val = poll["notation"], pollutant_id_from_url(poll["id"])
        ids[key].add(val)
    return ids


def cities(*countries: str) -> defaultdict[str, set[str]]:
    """city names id and notation from API"""
    if not COUNTRY_CODES.issuperset(countries):
        unknown = sorted(set(countries) - COUNTRY_CODES)
        warn(
            f"Unknown country code(s) {', '.join(unknown)}",
            UserWarning,
            stacklevel=2,
        )

    payload = asyncio.run(json_from_post_api("City", countries))
    cities: defaultdict[str, set[str]] = defaultdict(set)
    for city in payload:
        key, val = city["countryCode"], city["cityName"]
        cities[key].add(val)
    return cities
