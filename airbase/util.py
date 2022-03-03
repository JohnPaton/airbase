"""Utility functions for processing the raw Portal responses, url templating, etc."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, overload

from .resources import (
    ALL_SOURCES,
    CURRENT_YEAR,
    DATE_FMT,
    LINK_LIST_URL_TEMPLATE,
)


@overload
def string_safe_list(obj: None) -> list[None]:  # pragma: no cover
    ...


@overload
def string_safe_list(obj: str | Iterable[str]) -> list[str]:  # pragma: no cover
    ...


def string_safe_list(obj: str | Iterable[str] | None) -> list[str] | list[None]:
    """
    Turn an (iterable) object into a list. If it is a string or not
    iterable, put the whole object into a list of length 1.

    :param obj:
    :return list:
    """
    if isinstance(obj, str):
        return [obj]
    if obj is None:
        return [obj]
    return list(obj)


def link_list_url(
    country: str | None,
    shortpl: str | None = None,
    year_from: str = "2013",
    year_to: str = CURRENT_YEAR,
    source: str = "All",
    update_date: str | datetime | None = None,
) -> str:
    """
    Generate the URL where the download links for a query can be found.

    :param country: The 2-letter country code. See
        AirbaseClient.countries for options.
    :param shortpl: (optional) The pollutant number. Leave blank to
        get all pollutants. See AirbaseClient.pollutants_per_country for
        options.
    :param year_from: (optional) The first year of data. Can not be
        earlier than 2013. Default 2013.
    :param year_to: (optional) The last year of data. Can not be
        later than the current year. Default <current year>.
    :param source: (optional) One of "E1a", "E2a" or "All". E2a
        (UTD) data are only available for years where E1a data have not
        yet been delivered (this will normally be the most recent year).
        Default "All".
    :param update_date: (optional). Format
        "yyyy-mm-dd hh:mm:ss". To be used when only files created or
        updated after a certain date is of interest.

    :return: The URL which will yield the list of relevant CSV
        download links.
    """
    if int(year_from) < 2013:
        raise ValueError("'year_from' must be at least 2013")
    year_from = str(int(year_from))

    if int(year_to) > int(CURRENT_YEAR):
        raise ValueError("'year_to' must be at most " + str(CURRENT_YEAR))
    year_to = str(int(year_to))

    if isinstance(update_date, datetime):
        update_date = update_date.strftime(DATE_FMT)
    update_date = update_date or ""

    if source is not None and source not in ALL_SOURCES:
        raise ValueError("'source' must be one of: " + ",".join(ALL_SOURCES))
    source = source or ""

    return LINK_LIST_URL_TEMPLATE.format(
        country=country or "",
        shortpl=shortpl or "",
        year_from=year_from,
        year_to=year_to,
        source=source,
        update_date=update_date,
    )
