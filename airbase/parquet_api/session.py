from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Iterable, Iterator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import TracebackType
from typing import TypeVar
from warnings import warn

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover

import aiohttp
from async_property import async_cached_property
from tqdm import tqdm

from ..summary import DB
from .client import Client
from .dataset import (
    AggregationType,
    Dataset,
    ParquetData,
    request_info_by_city,
    request_info_by_country,
)

_T = TypeVar("_T")


class Session(AbstractAsyncContextManager):
    client: Client = Client()

    def __init__(
        self,
        *,
        progress: bool = False,
        raise_for_status: bool = True,
    ) -> None:
        """
        :param progress: (optional, default `False`)
            Show progress bars
        :param raise_for_status: (optional, default `True`)
            Raise exceptions if any request from `summary`, `url_to_files` or `download_to_directory`
            methods returns "bad" HTTP status codes.
            If False, a :py:func:`warnings.warn` will be issued instead. Default True.
        """
        self.progress = progress
        self.raise_for_status = raise_for_status
        self._expected_files: int = 0
        self._expected_size: int = 0  # in bytes
        self._urls_to_download: set[str] = set()

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
        self.clear()

    @property
    def expected_files(self) -> int:
        """expected number of files to download"""
        return self._expected_files

    @expected_files.setter
    def expected_files(self, n: int) -> None:
        """expected number of files to download"""
        self._expected_files = n

    @property
    def expected_size(self) -> int:
        """expected download size in Mb"""
        return self._expected_size // (1_024 * 1_024)

    @expected_size.setter
    def expected_size(self, n: int) -> None:
        """expected download size in Mb"""
        self._expected_size = n

    def add_expected(self, numberFiles: int, size: int) -> None:
        """add to the expected download files and size in Mb"""
        self._expected_files += numberFiles
        self._expected_size += size * 1_024 * 1_024

    @property
    def number_of_urls(self) -> int:
        """number of unique URLs ready for download"""
        return len(self._urls_to_download)

    @property
    def urls(self) -> Iterable[str]:
        """unique URLs ready for download"""
        yield from self._urls_to_download

    def add_urls(self, more_urls: Iterable[str]) -> int:
        """add to the unique URLs ready for download"""
        old_urls = self.number_of_urls
        urls = (u.strip() for u in more_urls)
        self._urls_to_download.update(
            u for u in urls if u.startswith(("http://", "https://"))
        )
        return self.number_of_urls - old_urls

    def remove_url(self, url: str) -> None:
        """remove URL from unique URLs ready for download"""
        self._urls_to_download.remove(url)

    def clear(self) -> None:
        """reset URLs and expected download values"""
        self._expected_files = 0
        self._expected_size = 0
        self._urls_to_download.clear()

    @async_cached_property
    async def countries(self) -> list[str]:
        """request country codes from API"""
        payload = await self.client.country()
        return [country["countryCode"] for country in payload]

    @async_cached_property
    async def pollutants(self) -> defaultdict[str, set[int]]:
        """requests pollutants id and notation from API"""

        payload = await self.client.pollutant()
        ids: defaultdict[str, set[int]] = defaultdict(set)
        for poll in payload:
            key, val = poll["notation"], pollutant_id_from_url(poll["id"])
            ids[key].add(val)
        return ids

    async def cities(self, *countries: str) -> defaultdict[str, set[str]]:
        """city names id and notation from API"""
        if not DB.COUNTRY_CODES.issuperset(countries):
            unknown = sorted(set(countries) - DB.COUNTRY_CODES)
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

    async def summary(self, *download_infos: ParquetData) -> None:
        """
        aggregated summary from multiple requests

        :param download_infos: info about requested urls
        """
        unique_info = set(download_infos)
        with tqdm(
            desc="summary".ljust(8),
            unit="requests",
            total=len(unique_info),
            disable=not self.progress,
        ) as progress:
            async for summary in self.__completed(
                self.client.download_summary(info.payload())
                for info in unique_info
            ):
                progress.update()
                self.add_expected(**summary)

    async def url_to_files(self, *download_infos: ParquetData) -> None:
        """
        multiple request for file URLs and return only unique URLs from each responses

        :param download_infos: info about requested urls
        """
        unique_info = set(download_infos)
        if self.progress and self.expected_files == 0:
            await self.summary(*unique_info)

        with tqdm(
            desc="URLs".ljust(8),
            unit="URL",
            unit_scale=True,
            total=self.expected_files,
            disable=not self.progress,
        ) as progress:
            async for text in self.__completed(
                self.client.download_urls(info.payload())
                for info in unique_info
            ):
                new_urls = self.add_urls(text.strip().splitlines())
                progress.update(new_urls)

    async def download_to_directory(
        self,
        root_path: Path,
        *,
        country_subdir: bool = True,
        skip_existing: bool = True,
    ) -> None:
        """
        download into a directory

        :param root_path: The directory to save files in (must exist)
        :param country_subdir: (optional, default `True`)
            Download files for different counties to different `root_path` sub directories.
            If False, download all files to `root_path`
        :param skip_existing: (optional, default `True`)
            Don't re-download files if they exist in `root_path`.
            If False, existing files in `root_path` may be overwritten.
            Empty files will be re-downloaded regardless of this option.

        NOTE
        need to call `url_to_files` first, in order to retrieve the URLs to download, or
        add the urls directly with `add_urls`
        """

        if not root_path.is_dir():  # pragma: no cover
            raise NotADirectoryError(
                f"{root_path.resolve()} is not a directory."
            )

        if self.number_of_urls < 1:
            warn(
                f"No URLs to download, call {self.__class__.__name__}.url_to_files before calling this method"
                f" or add the urls directly with {self.__class__.__name__}.add_urls",
                UserWarning,
                stacklevel=2,
            )
            return

        n = -2 if country_subdir else -1
        paths: dict[Path, str] = {
            root_path.joinpath(*url.split("/")[n:]): url for url in self.urls
        }
        if skip_existing:
            existing = {
                path: size
                for path in paths
                # re-download empty files
                if path.is_file() and (size := path.stat().st_size) >= 0
            }
            for path in existing:
                url = paths.pop(path)
                self.remove_url(url)
            self.add_expected(
                numberFiles=-len(existing),
                size=-sum(existing.values()) // (1_024 * 1_024),
            )

        # create missing country sub-directories before downloading
        for parent in {path.parent for path in paths}:
            parent.mkdir(exist_ok=True)

        with tqdm(
            desc="download".ljust(8),
            unit="b",
            unit_scale=True,
            unit_divisor=1_024,
            total=self.expected_size * 1_024 * 1_024,
            disable=not self.progress,
        ) as progress:
            async for path in self.__completed(
                self.client.download_binary(url, path)
                for path, url in paths.items()
            ):
                assert path.is_file(), f"missing {path.name}"
                progress.update(path.stat().st_size)
                url = paths.pop(path)
                self.remove_url(url)

        assert not paths, "still some paths to download"
        assert self.number_of_urls == 0, "still some URLs to download"
        self.expected_files = 0
        self.expected_size = 0

    async def download_metadata(
        self,
        path: Path,
        skip_existing: bool = True,
    ) -> None:
        """
        download station metadata into the given `path`.

        :param path: :py:class:`pathlib.Path` to the station metadata (parent directory must exist)
        :param skip_existing: (optional, default `True`)
            Don't re-download metadata if `path` already exists.
            If False, `path` may be overwritten.
        """
        if not path.parent.is_dir():  # pragma: no cover
            raise NotADirectoryError(
                f"{path.parent.resolve()} is not a directory."
            )

        if skip_existing and path.exists():
            return

        if self.progress:
            tqdm.write(f"downloading station metadata to {path}")
        await self.client.download_metadata(path)

    async def __completed(
        self, jobs: Iterator[Awaitable[_T]]
    ) -> AsyncIterator[_T]:
        aws = tuple(jobs)
        for future in asyncio.as_completed(aws):
            try:
                yield await future
            except asyncio.CancelledError:  # pragma:no cover
                continue
            except aiohttp.ClientResponseError as e:  # pragma:no cover
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
    countries: frozenset[str] | set[str],
    pollutants: frozenset[str] | set[str] | None = None,
    cities: frozenset[str] | set[str] | None = None,
    frequency: AggregationType | None = None,
    summary_only: bool = False,
    metadata: bool = False,
    country_subdir: bool = True,
    overwrite: bool = False,
    quiet: bool = True,
    raise_for_status: bool = False,
    session: Session = Session(),
):
    """
    request file urls by country|city/pollutant and download unique files

    :param dataset: `Dataset.Historical`, `Dataset.Verified` or `Dataset.Unverified`.
    :param root_path: The directory to save files in (must exist).
    :param countries: Request observations for these countries.
    :param pollutants: (optional, default `None`)
        Limit requests to these specific pollutants.
    :param cities: (optional, default `None`)
        Limit requests to these specific cities.
    :param summary_only: (optional, default `False`)
        Request total files/size, nothing will be downloaded.
    :param metadata: (optional, default `False`)
        Download station metadata into `root_path/"metadata.csv"`.
    :param country_subdir: (optional, default `True`)
        Download files for different counties to different `root_path` sub directories.
        If False, download all files to `root_path`
    :param overwrite: (optional, default `False`)
        Re-download existing files in `root_path`.
        If False, existing files will be skipped.
        Empty files will be re-downloaded regardless of this option.
    :param quiet: (optional, default `True`)
        Disable progress bars.
    :param raise_for_status: (optional, default `False`)
        Raise exceptions if any request return "bad" HTTP status codes.
        If False, a :py:func:`warnings.warn` will be issued instead.
    """
    if cities:  # one request for each city/pollutant
        info = request_info_by_city(
            dataset, *cities, pollutants=pollutants, frequency=frequency
        )
    else:  # one request for each country/pollutant
        if not countries:
            countries = DB.COUNTRY_CODES
        info = request_info_by_country(
            dataset, *countries, pollutants=pollutants, frequency=frequency
        )

    if not info:
        warn(
            "No data to download, please check the download options",
            UserWarning,
        )
        return

    session.progress = not quiet
    session.raise_for_status = raise_for_status
    if summary_only:
        async with session:
            await session.summary(*info)
            print(
                f"found {session.expected_files:_} file(s), ~{session.expected_size:_} Mb in total",
                file=sys.stderr,
            )
        return

    async with session:
        if metadata:
            await session.download_metadata(
                root_path / "metadata.csv",
                skip_existing=not overwrite,
            )

        await session.url_to_files(*info)
        if session.number_of_urls == 0:
            warn(
                "Found no data matching your selection, please try different cites/pollutants"
                if cities
                else "Found no data matching your selection, please try different pollutants",
                UserWarning,
            )
            return

        await session.download_to_directory(
            root_path,
            country_subdir=country_subdir,
            skip_existing=not overwrite,
        )
