from __future__ import annotations

import asyncio
import sys
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
from tqdm import tqdm

from ..summary import COUNTRY_CODES
from .client import Client
from .dataset import (
    CSVData,
    Source,
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

    @property
    def number_of_urls(self) -> int:
        """number of unique URLs ready for download"""
        return len(self._urls_to_download)

    @property
    def urls(self) -> Iterable[str]:
        """unique URLs ready for download"""
        yield from self._urls_to_download

    def add_urls(self, more_urls: Iterable[str]) -> None:
        """add to the unique URLs ready for download"""
        urls = (u.strip() for u in more_urls)
        self._urls_to_download.update(
            u for u in urls if u.startswith(("http://", "https://"))
        )

    def remove_url(self, url: str) -> None:
        """remove URL from unique URLs ready for download"""
        self._urls_to_download.remove(url)

    async def url_to_files(self, *download_infos: CSVData) -> int:
        """
        multiple request for file URLs and return only unique URLs from each responses

        :param download_infos: info about requested urls

        :return: number of unique URLs ready for download
        """
        unique_info = set(download_infos)
        with tqdm(
            desc="URLs".ljust(8),
            unit="URL",
            unit_scale=True,
            disable=not self.progress,
            total=len(unique_info),
        ) as progress:
            async for text in self.__completed(
                self.client.download_urls(info.param()) for info in unique_info
            ):
                progress.update()
                self.add_urls(text.strip().splitlines())

        return self.number_of_urls

    async def download_to_directory(
        self,
        root_path: Path,
        skip_existing: bool = True,
    ) -> None:
        """
        download into a directory, files for different counties are kept on different sub directories

        :param root_path: The directory to save files in (must exist)
        :param skip_existing: (optional) Don't re-download files if they exist in `root_path`.
            If False, existing files in `root_path` may be overwritten.
            Empty files will be re-downloaded regardless of this option. Default True.

        NOTE
        need to call `url_to_files` first, in order to retrieve the URLs to download
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

        paths: dict[Path, str] = {
            root_path.joinpath(*url.split("/")[-2:]): url for url in self.urls
        }
        if skip_existing:
            existing = (
                path
                for path in paths
                # re-download empty files
                if path.is_file() and path.stat().st_size >= 0
            )
            for path in list(existing):
                url = paths.pop(path)
                self.remove_url(url)

        # create missing country sub-directories before downloading
        for parent in {path.parent for path in paths}:
            parent.mkdir(exist_ok=True)

        with tqdm(
            desc="download".ljust(8),
            unit="files",
            unit_scale=True,
            disable=not self.progress,
            total=len(paths),
        ) as progress:
            async for path in self.__completed(
                self.client.download_binary(url, path)
                for path, url in paths.items()
            ):
                progress.update()
                assert path.is_file(), f"missing {path.name}"
                url = paths.pop(path)
                self.remove_url(url)

        assert not paths, "still some paths to download"
        assert self.number_of_urls == 0, "still some URLs to download"

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


async def download(
    source: Source,
    year: int,
    root_path: Path,
    *,
    countries: frozenset[str] | set[str],
    pollutants: frozenset[str] | set[str] | None = None,
    cities: frozenset[str] | set[str] | None = None,
    metadata: bool = False,
    overwrite: bool = False,
    quiet: bool = True,
    raise_for_status: bool = False,
    session: Session = Session(),
):
    """
    request file urls by country|city/pollutant and download unique files

    :param quiet: (optional, default `True`)
        Disable progress bars.
    :param raise_for_status: (optional, default `False`)
        Raise exceptions if any request return "bad" HTTP status codes.
        If False, a :py:func:`warnings.warn` will be issued instead

    """
    if cities:  # one request for each city/pollutant
        info = request_info_by_city(
            source, year, *cities, pollutants=pollutants
        )
    else:  # one request for each country/pollutant
        if not countries:
            countries = COUNTRY_CODES
        info = request_info_by_country(
            source, year, *countries, pollutants=pollutants
        )

    if not info:
        warn("No data to download, please check the download options")
        return

    session.progress = not quiet
    session.raise_for_status = raise_for_status
    async with session:
        if metadata:
            await session.download_metadata(
                root_path / "metadata.tsv",
                skip_existing=not overwrite,
            )

        await session.url_to_files(*info)
        await session.download_to_directory(
            root_path,
            skip_existing=not overwrite,
        )
