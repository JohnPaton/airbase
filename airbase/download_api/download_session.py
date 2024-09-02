from __future__ import annotations

import asyncio
import sys
from collections import Counter, defaultdict
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import TracebackType
from warnings import warn

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover


from async_property import async_cached_property
from tqdm import tqdm

from ..summary import COUNTRY_CODES
from .abstract_api_client import AbstractClient, DownloadSummaryJSON
from .api_client import (
    Dataset,
    ParquetData,
    request_info_by_city,
    request_info_by_country,
)
from .concrete_api_client import Client, ClientResponseError


class DownloadSession(AbstractAsyncContextManager):
    client: AbstractClient = Client()

    def __init__(
        self,
        *,
        progress: bool = False,
        raise_for_status: bool = True,
        custom_client: AbstractClient | None = None,
    ) -> None:
        """
        :param progress: (optional, default `False`)
            Show progress bars
        :param raise_for_status: (optional, default `True`)
            Raise exceptions if any request from `summary`, `url_to_files` or `download_to_directory`
            methods returns "bad" HTTP status codes.
            If False, a :py:func:`warnings.warn` will be issued instead. Default True.
        """

        if custom_client is not None:
            self.client = custom_client

        self.progress = progress
        self.raise_for_status = raise_for_status

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)

    @async_cached_property
    async def countries(self) -> list[str]:
        """request country codes from API"""
        payload = await self.client.country()
        return [country["countryCode"] for country in payload]

    @async_cached_property
    async def pollutants(self) -> defaultdict[str, set[int]]:
        """requests pollutants id and notation from API"""

        payload = await self.client.property()
        ids: defaultdict[str, set[int]] = defaultdict(set)
        for poll in payload:
            key, val = poll["notation"], pollutant_id_from_url(poll["id"])
            ids[key].add(val)
        return ids

    async def cities(self, *countries: str) -> defaultdict[str, set[str]]:
        """city names id and notation from API"""
        if not COUNTRY_CODES.issuperset(countries):
            unknown = sorted(set(countries) - COUNTRY_CODES)
            warn(
                f"Unknown country code(s) {', '.join(unknown)}",
                UserWarning,
                stacklevel=2,
            )

        payload = await self.client.city(countries)
        cities: defaultdict[str, set[str]] = defaultdict(set)
        for city in payload:
            key, val = city["countryCode"], city["cityName"]
            cities[key].add(val)
        return cities

    async def summary(
        self, *download_infos: ParquetData
    ) -> DownloadSummaryJSON:
        """
        aggregated summary from multiple requests

        :param urls: info about requested urls
        :param progress: show progress bar
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.

        :return: total number of files and file size
        """
        unique_info = set(download_infos)
        jobs = tuple(
            self.client.download_summary(info.payload()) for info in unique_info
        )

        total: Counter[str] = Counter()
        with tqdm(
            initial=len(download_infos) - len(unique_info),
            total=len(download_infos),
            leave=True,
            disable=not self.progress,
            desc="totalize",
        ) as progress_bar:
            for future in asyncio.as_completed(jobs):
                progress_bar.update()
                try:
                    summary = await future
                except asyncio.CancelledError:  # pragma:no cover
                    continue
                except ClientResponseError as e:  # pragma:no cover
                    if self.raise_for_status:
                        raise
                    warn(str(e), category=RuntimeWarning)
                else:
                    total.update(summary)

        return dict(total)  # type:ignore[return-value]

    async def url_to_files(
        self, *download_infos: ParquetData
    ) -> AsyncIterator[set[str]]:
        """
        multiple request for file URLs and return only unique URLs from each responses


        :param urls: info about requested urls
        :param progress: show progress bar
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.

        :return: unique file URLs among from all the responses
        """
        unique_info = set(download_infos)
        jobs = tuple(
            self.client.download_urls(info.payload()) for info in unique_info
        )

        with tqdm(
            initial=len(download_infos) - len(unique_info),
            total=len(download_infos),
            leave=True,
            disable=not self.progress,
            desc="generate",
        ) as progress_bar:
            for future in asyncio.as_completed(jobs):
                progress_bar.update()
                try:
                    text = await future
                except asyncio.CancelledError:  # pragma:no cover
                    continue
                except ClientResponseError as e:  # pragma:no cover
                    if self.raise_for_status:
                        raise
                    warn(str(e), category=RuntimeWarning)
                else:
                    lines = (line.strip() for line in text.splitlines())
                    if urls := set(
                        line
                        for line in lines
                        if line.startswith(("http://", "https://"))
                    ):
                        yield urls

    async def download_to_directory(
        self, root_path: Path, *urls: str, skip_existing: bool = True
    ) -> None:
        """
        download into a directory, files for different counties are kept on different sub directories

        :param root_path: The directory to save files in (must exist)
        :param urls: urls to files to download
        :param skip_existing: (optional) Don't re-download files if they exist in `root_path`.
            If False, existing files in `root_path` may be overwritten.
            Empty files will be re-downloaded regardless of this option. Default True.
        """

        if not root_path.is_dir():  # pragma: no cover
            raise NotADirectoryError(
                f"{root_path.resolve()} is not a directory."
            )

        url_paths: dict[str, Path] = {
            url: root_path / "/".join(url.split("/")[-2:]) for url in urls
        }
        if skip_existing:
            url_paths = {
                url: path
                for url, path in url_paths.items()
                # re-download empty files
                if not path.is_file() or path.stat().st_size == 0
            }

        # create missing country sum-directories before downloading
        for parent in {path.parent for path in url_paths.values()}:
            parent.mkdir(exist_ok=True)

        jobs = tuple(
            self.client.download_binary(url, path)
            for url, path in url_paths.items()
        )

        with tqdm(
            initial=len(jobs) - len(url_paths),
            total=len(url_paths),
            leave=True,
            disable=not self.progress,
            desc="download",
        ) as progress_bar:
            for future in asyncio.as_completed(jobs):
                progress_bar.update()
                try:
                    path = await future
                except asyncio.CancelledError:  # pragma:no cover
                    continue
                except ClientResponseError as e:  # pragma:no cover
                    if self.raise_for_status:
                        raise
                    warn(str(e), category=RuntimeWarning)
                else:
                    assert path.is_file(), f"missing {path.name}"


def pollutant_id_from_url(url: str) -> int:
    """
    numeric pollutant id from urls like
        http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1
        http://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/44/view
    """
    if url.endswith("view"):
        return int(url.split("/")[-2])
    return int(url.split("/")[-1])


async def download(
    dataset: Dataset,
    root_path: Path,
    *,
    countries: set[str],
    pollutants: set[str] | None = None,
    cities: set[str] | None = None,
    summary_only: bool = False,
    overwrite: bool = False,
    quiet: bool = True,
    raise_for_status: bool = False,
    session: DownloadSession = DownloadSession(),
):
    """
    request file urls by country|[city]/pollutant and download unique files

    :param quiet: (optional, default `True`)
        Disable progress bars.
    :param raise_for_status: (optional, default `False`)
        Raise exceptions if any request return "bad" HTTP status codes.
        If False, a :py:func:`warnings.warn` will be issued instead

    """
    if cities:  # one request for each city/pollutant
        info = request_info_by_city(dataset, *cities, pollutant=pollutants)
    else:  # one request for each country/pollutant
        if not countries:
            countries = COUNTRY_CODES
        info = request_info_by_country(
            dataset, *countries, pollutant=pollutants
        )

    session.progress = not quiet
    session.raise_for_status = raise_for_status
    async with session:
        summary = await session.summary(*info)
        if summary_only:
            print(
                "found {numberFiles:_} file(s), ~{size:_} MB in total".format_map(
                    summary
                )
            )
            return

        async for urls in session.url_to_files(*info):
            await session.download_to_directory(
                root_path,
                *urls,
                skip_existing=not overwrite,
            )
