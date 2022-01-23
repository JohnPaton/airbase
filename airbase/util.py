"""Utility functions for processing the raw Portal responses, url templating, etc."""
from __future__ import annotations

import datetime
from copy import deepcopy
from typing import Iterable, overload

from .resources import (
    ALL_SOURCES,
    CURRENT_YEAR,
    DATE_FMT,
    LINK_LIST_URL_TEMPLATE,
)


@overload
def string_safe_list(obj: None) -> list[None]:
    ...


@overload
def string_safe_list(obj: str | Iterable[str]) -> list[str]:
    ...


def string_safe_list(obj):  # type:ignore[no-untyped-def]
    """
    Turn an (iterable) object into a list. If it is a string or not
    iterable, put the whole object into a list of length 1.

    :param obj:
    :return list:
    """
    if isinstance(obj, str) or obj is None:
        return [obj]
    return list(obj)


def countries_from_summary(summary: list[dict[str, str]]) -> list[str]:
    """
    Get the list of unique countries from the summary.

    :param list[dict] summary: The E1a summary.

    :return list[str]: The available countries.
    """
    return list(set(d["ct"] for d in summary))


def pollutants_from_summary(summary: list[dict[str, str]]) -> dict[str, str]:
    """
    Get the list of unique pollutants from the summary.

    :param list[dict] summary: The E1a summary.

    :return dict: The available pollutants, with name ("pl") as key
        and pollutant number ("shortpl") as value.
    """
    return {d["pl"]: d["shortpl"] for d in summary}


def pollutants_per_country(
    summary: list[dict[str, str]]
) -> dict[str, list[dict[str, str]]]:
    """
    Get the available pollutants per country from the summary.

    :param list[dict] summary: The E1a summary.

    :return dict[list[dict]]: All available pollutants per country.
    """
    output: dict[str, list[dict[str, str]]] = dict()

    for d in deepcopy(summary):
        country = d.pop("ct")

        if country in output:
            output[country].append(d)
        else:
            output[country] = [d]

    return output


def link_list_url(
    country: str,
    shortpl: str | None = None,
    year_from: str = "2013",
    year_to: str = CURRENT_YEAR,
    source: str = "All",
    update_date: str | datetime.datetime | None = None,
) -> str:
    """
    Generate the URL where the download links for a query can be found.

    :param str country: The 2-letter country code. See
        AirbaseClient.countries for options.
    :param str shortpl: (optional) The pollutant number. Leave blank to
        get all pollutants. See AirbaseClient.pollutants_per_country for
        options.
    :param str year_from: (optional) The first year of data. Can not be
        earlier than 2013. Default 2013.
    :param str year_to: (optional) The last year of data. Can not be
        later than the current year. Default <current year>.
    :param str source: (optional) One of "E1a", "E2a" or "All". E2a
        (UTD) data are only available for years where E1a data have not
        yet been delivered (this will normally be the most recent year).
        Default "All".
    :param str|datetime update_date: (optional). Format
        "yyyy-mm-dd hh:mm:ss". To be used when only files created or
        updated after a certain date is of interest.

    :return str: The URL which will yield the list of relevant CSV
        download links.
    """
    if shortpl is None:
        shortpl = ""

    if not (2013 <= int(year_from) <= int(CURRENT_YEAR)):
        raise ValueError(
            f"'year_from' most contain a year between '2013' and '{CURRENT_YEAR}'"
        )
    year_from = str(int(year_from))

    if not (2013 <= int(year_to) <= int(CURRENT_YEAR)):
        raise ValueError(
            f"'year_to' most contain a year between '2013' and '{CURRENT_YEAR}'"
        )
    year_to = str(int(year_to))

    if isinstance(update_date, datetime.datetime):
        update_date = update_date.strftime(DATE_FMT)
    update_date = update_date or ""

    if source is not None and source not in ALL_SOURCES:
        raise ValueError("'source' must be one of: " + ",".join(ALL_SOURCES))
    source = source or ""

    return LINK_LIST_URL_TEMPLATE.format(
        country=country,
        shortpl=shortpl,
        year_from=year_from,
        year_to=year_to,
        source=source,
        update_date=update_date,
    )


def extract_csv_links(text: str) -> list[str]:
    """Get a list of csv links from the download link response text"""
    links = text.replace("\r", "").split("\n")
    links.remove("")
    return links
