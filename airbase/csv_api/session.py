from __future__ import annotations

import asyncio
import sys
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

import aiohttp
from tqdm import tqdm

from .client import Client
from .dataset import CSVData

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
    def urls_to_download(self) -> int:
        """number of unique URLs ready for download"""
        return len(self._urls_to_download)

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
                lines = (line.strip() for line in text.splitlines())
                if urls := set(
                    line
                    for line in lines
                    if line.startswith(("http://", "https://"))
                ):
                    self._urls_to_download.update(urls)

        return self.urls_to_download

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

        if self._urls_to_download:
            warn(
                f"No URLs to download, call {self.__class__.__name__}.url_to_files before calling this method",
                UserWarning,
                stacklevel=2,
            )
            return

        paths: dict[Path, str] = {
            root_path / "/".join(url.split("/")[-2:]): url
            for url in self._urls_to_download
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
                self._urls_to_download.remove(url)

        # create missing country sum-directories before downloading
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
                self._urls_to_download.remove(url)

        assert not paths, "still some paths to download"
        assert not self._urls_to_download, "still some URLs to download"

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
