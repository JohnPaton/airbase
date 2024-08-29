from __future__ import annotations

import sys
from collections import Counter, defaultdict
from contextlib import AbstractAsyncContextManager
from itertools import product
from pathlib import Path
from types import TracebackType
from warnings import warn

if sys.version_info >= (3, 11):  # pragma:no cover
    from typing import Self
else:
    from typing_extensions import Self  # pragma:no cover


import aiocache
from tqdm.asyncio import tqdm

from ..summary import COUNTRY_CODES
from .api_client import Dataset, DownloadAPI, DownloadInfo, DownloadSummaryDict


class DownloadSession(AbstractAsyncContextManager):
    client = DownloadAPI()

    def __init__(self, *, custom_client: DownloadAPI | None = None) -> None:
        if custom_client is not None:
            self.client = custom_client

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

    @aiocache.cached()
    async def countries(self) -> list[str]:
        """request country codes from API"""
        payload = await self.client.country()
        return [country["countryCode"] for country in payload]

    @aiocache.cached()
    async def pollutants(self) -> defaultdict[str, set[int]]:
        """requests pollutants id and notation from API"""

        payload = await self.client.property()
        ids: defaultdict[str, set[int]] = defaultdict(set)
        for poll in payload:
            key, val = poll["notation"], pollutant_id_from_url(poll["id"])
            ids[key].add(val)
        return ids

    @aiocache.cached()
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
        self,
        *info: DownloadInfo,
        progress: bool = False,
        raise_for_status: bool = True,
    ) -> DownloadSummaryDict:
        """
        aggregated summary from multiple requests

        :param urls: info about requested urls
        :param progress: show progress bar
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.

        :return: total number of files and file size
        """
        unique_info = set(info)
        total: Counter[str] = Counter()
        async for summary in tqdm(
            self.client.download_summary(
                unique_info,
                raise_for_status=raise_for_status,
            ),
            initial=len(info) - len(unique_info),
            total=len(info),
            leave=True,
            disable=not progress,
        ):
            total.update(summary)

        return dict(total)  # type:ignore[return-value]

    async def url_to_files(
        self,
        *info: DownloadInfo,
        progress: bool = False,
        raise_for_status: bool = True,
    ) -> set[str]:
        """
        multiple request for file URLs and return only the unique URLs among all the responses

        :param urls: info about requested urls
        :param progress: show progress bar
        :param raise_for_status: Raise exceptions if download links
            return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead.

        :return: unique file URLs among from all the responses
        """
        unique_info = set(info)
        unique_urls: set[str] = set()
        async for urls in tqdm(
            self.client.download_urls(
                unique_info,
                raise_for_status=raise_for_status,
            ),
            initial=len(info) - len(unique_info),
            total=len(info),
            leave=True,
            disable=not progress,
        ):
            unique_urls.update(urls)
        return unique_urls

    async def download_to_directory(
        self,
        root_path: Path,
        *urls: str,
        skip_existing: bool = True,
        progress: bool = False,
        raise_for_status: bool = True,
    ) -> None:
        """
        download into a directory, files for different counties are kept on different sub directories

        :param root_path: The directory to save files in (must exist)
        :param urls: urls to files to download
        :param skip_existing: (optional) Don't re-download files if they exist in `root_path`.
            If False, existing files in `root_path` may be overwritten.
            Empty files will be re-downloaded regardless of this option. Default True.
        :param progress: show progress bar
        :param raise_for_status: (optional) Raise exceptions if
            download links return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead. Default True.
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

        async for path in tqdm(
            self.client.download_binary_files(
                url_paths, raise_for_status=raise_for_status
            ),
            initial=len(urls) - len(url_paths),
            total=len(urls),
            leave=True,
            disable=not progress,
        ):
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
    countries: list[str],
    pollutants: list[str],
    cities: list[str],
    summary_only: bool = False,
    overwrite: bool = False,
    quiet: bool = True,
    session: DownloadSession = DownloadSession(),
):
    """
    request file urls by pollutant/country[/city] and download unique files
    """
    async with session:
        if cities:
            # one request for each country/pollutant/city
            country_cities = await session.cities(*countries)
            if not countries:
                countries = list(country_cities)
            if not pollutants:
                pollutants = [None]  # type:ignore[list-item]

            info = (
                DownloadInfo(pollutant, country, dataset, city)
                for pollutant, country, city in product(
                    pollutants, countries, cities
                )
                if city in country_cities[country]
            )
        else:
            # one request for each country/pollutant
            if not countries:
                countries = list(await session.countries())
            if not pollutants:
                pollutants = list(await session.pollutants())

            info = (
                DownloadInfo(pollutant, country, dataset)
                for pollutant, country in product(pollutants, countries)
            )

        if summary_only:
            summary = await session.summary(*info, progress=not quiet)
            print(
                "found {numberFiles} file(s), ~{size} MB in total".format_map(
                    summary
                )
            )
        else:
            urls = await session.url_to_files(*info, progress=not quiet)
            await session.download_to_directory(
                root_path,
                *urls,
                skip_existing=not overwrite,
                progress=not quiet,
            )
