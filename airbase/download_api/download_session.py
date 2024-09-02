from __future__ import annotations

import asyncio
import sys
from collections import Counter, defaultdict
from collections.abc import AsyncIterator, Awaitable, Iterator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import TracebackType
from typing import TypeVar
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

_T = TypeVar("_T")


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

        """files/Mb downloaded so far"""
        self.count: Counter[str] = Counter()

        """files/Mb in total"""
        self.total: Counter[str] = Counter()

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        self.count.clear()
        self.total.clear()
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

        :return: total number of files and file size
        """
        unique_info = set(download_infos)
        with tqdm(
            desc="totalize",
            unit="requests",
            total=len(download_infos),
            disable=not self.progress,
        ) as progress_bar:
            async for summary in self.__completed(
                self.client.download_summary(info.payload())
                for info in unique_info
            ):
                progress_bar.update()
                self.total.update(numberRequests=1, **summary)

        return dict(
            numberFiles=self.total["numberFiles"], size=self.total["size"]
        )

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
        self.count.update(numberFiles=len(download_infos) - len(unique_info))
        if not self.total["numberFiles"]:  # did not run summary before
            self.total.update(numberFiles=len(download_infos))

        with tqdm(
            desc="generate",
            unit="files",
            unit_scale=True,
            initial=self.count["numberFiles"],
            total=self.total["numberFiles"],
            disable=not self.progress,
        ) as progress_bar:
            async for text in self.__completed(
                self.client.download_urls(info.payload())
                for info in unique_info
            ):
                self.count.update(numberRequests=1)
                lines = (line.strip() for line in text.splitlines())
                if urls := set(
                    line
                    for line in lines
                    if line.startswith(("http://", "https://"))
                ):
                    progress_bar.update(numberFiles := len(urls))
                    self.count.update(numberFiles=numberFiles)
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
            existing = {
                url: path
                for url, path in url_paths.items()
                if path.is_file() and path.stat().st_size >= 0
            }
            for url, path in existing.items():
                # re-download empty files
                self.count.update(size=path.stat().st_size / 1_048_576)  # type:ignore[call-overload]
                del url_paths[url]

        # create missing country sum-directories before downloading
        for parent in {path.parent for path in url_paths.values()}:
            parent.mkdir(exist_ok=True)

        with tqdm(
            desc="download",
            unit="b",
            unit_scale=True,
            unit_divisor=1_024,
            initial=self.count["size"],
            total=self.total["size"],
            disable=not self.progress,
            leave=self.count["numberRequests"] >= self.total["numberRequests"],
        ) as progress_bar:
            async for path in self.__completed(
                self.client.download_binary(url, path)
                for url, path in url_paths.items()
            ):
                assert path.is_file(), f"missing {path.name}"
                progress_bar.update(size := path.stat().st_size)
                self.count.update(size=size / 1_048_576)  # type:ignore[call-overload]

    async def __completed(
        self, jobs: Iterator[Awaitable[_T]]
    ) -> AsyncIterator[_T]:
        aws = tuple(jobs)
        for future in asyncio.as_completed(aws):
            try:
                yield await future
            except asyncio.CancelledError:  # pragma:no cover
                continue
            except ClientResponseError as e:  # pragma:no cover
                if self.raise_for_status:
                    raise
                warn(str(e), category=RuntimeWarning)


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
        await session.summary(*info)
        if not summary_only:
            async for urls in session.url_to_files(*info):
                await session.download_to_directory(
                    root_path,
                    *urls,
                    skip_existing=not overwrite,
                )

    if summary_only:
        tqdm.write(
            "found {numberFiles:_} file(s), ~{size:_} Mb in total".format_map(
                session.total
            ),
            file=sys.stderr,
        )
    elif not quiet:
        tqdm.write(
            (
                "got {numberFiles:_} file(s), ~{size:_.0f} Mb".format_map(
                    session.count
                )
                + " from {numberFiles:_} file(s), ~{size:_} Mb".format_map(
                    session.total
                )
            ),
            file=sys.stderr,
        )
