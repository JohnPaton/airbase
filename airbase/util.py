"""Utility functions for processing the raw Portal responses, url templating, etc."""

import datetime
import queue
import time
import multiprocessing.dummy as mp

from .resources import (
    LINK_LIST_URL_TEMPLATE,
    CURRENT_YEAR,
    DATE_FMT,
    ALL_SOURCES,
)


def raise_for_error(e):
    """
    Raises an exception if passed.

    Useful as an error_callback for async operations.

    :param Exception e: the Exception in question
    """
    raise e


def string_safe_list(obj):
    """
    Turn an (iterable) object into a list. If it is a string or not
    iterable, put the whole object into a list of length 1.

    :param obj:
    :return list:
    """
    if isinstance(obj, str) or not hasattr(obj, "__iter__"):
        return [obj]
    else:
        return list(obj)


def countries_from_summary(summary):
    """
    Get the list of unique countries from the summary.

    :param list[dict] summary: The E1a summary.

    :return list[str]: The available countries.
    """
    return list({d["ct"] for d in summary})


def pollutants_from_summary(summary):
    """
    Get the list of unique pollutants from the summary.

    :param list[dict] summary: The E1a summary.

    :return dict: The available pollutants, with name ("pl") as key
        and pollutant number ("shortpl") as value.
    """
    return {d["pl"]: d["shortpl"] for d in summary}


def pollutants_per_country(summary):
    """
    Get the available pollutants per country from the summary.

    :param list[dict] summary: The E1a summary.

    :return dict[list[dict]]: All available pollutants per country.
    """
    output = dict()

    for d in summary.copy():
        country = d.pop("ct")

        if country in output:
            output[country].append(d)
        else:
            output[country] = [d]

    return output


def link_list_url(
    country,
    shortpl=None,
    year_from="2013",
    year_to=CURRENT_YEAR,
    source="All",
    update_date=None,
):
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
    shortpl = shortpl or ""

    if int(year_from) < 2013:
        raise ValueError("'year_from' must be at least 2013")
    year_from = str(int(year_from))

    if int(year_to) > int(CURRENT_YEAR):
        raise ValueError("'year_to' must be at most " + str(CURRENT_YEAR))
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


def extract_csv_links(text):
    """Get a list of csv links from the download link response text"""
    links = text.replace("\r", "").split("\n")
    links.remove("")
    return links


class _InterruptablePoolStopper:
    ...


def _func_apply_woker(func, input_q, error_q):
    while True:
        args = input_q.get()
        if args is _InterruptablePoolStopper:
            return
        try:
            func(*args)
        except Exception as e:
            error_q.put(e)


class InterruptablePool:
    def __init__(self, num_workers, func, func_args_iter):
        self._error_q = queue.Queue()
        self._args_q = queue.Queue(1)
        self._workers = []

        self.num_workers = num_workers
        self.func = func
        self.func_args_iter = func_args_iter

    def run(self):
        """Apply func to func_args_iter in parallel"""
        try:
            self._start()
            for args in self.func_args_iter:
                self._args_q.put(args)
                if not self._error_q.empty():
                    e = self._error_q.get()
                    self._stop()
                    raise ChildProcessError from e
        finally:
            self._stop()

    def _num_alive(self):
        """Current number of alive workers"""
        return sum([w.is_alive() for w in self._workers])

    def _start(self):
        """Start workers"""
        self._workers = [
            mp.Process(
                target=_func_apply_woker,
                args=(self.func, self._args_q, self._error_q),
            )
            for _ in range(self.num_workers)
        ]
        [w.start() for w in self._workers]

    def _stop(self):
        """Stop all alive workers"""
        try:
            self._args_q.get(block=False)
        except queue.Empty:
            pass

        [
            self._args_q.put(_InterruptablePoolStopper)
            for _ in range(self._num_alive())
        ]
