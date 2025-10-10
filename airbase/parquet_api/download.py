import asyncio
import sys
from collections.abc import Iterable
from pathlib import Path
from warnings import warn

from .dataset import ParquetData
from .session import Session


def metadata(session: Session, path: Path, *, overwrite: bool = False):
    """download station metadata.

    :param session:
        Parquet downloads API session.
    :param path:
        File path to write station metadata into.
    :param overwrite: (optional, default `False`)
        Re-download metadata.
        Empty files will be re-downloaded regardless of this option.
    """

    async def download():
        async with session:
            await session.download_metadata(path, skip_existing=not overwrite)

    asyncio.run(download())


def summary(session: Session, info: Iterable[ParquetData]):
    """request total files/size, nothing will be downloaded.

    :param session: Parquet downloads API session.
    :param info: requests by country|city/pollutant.
    """

    async def summary() -> tuple[int, int]:
        async with session:
            await session.summary(*info)
            return session.expected_files, session.expected_size

    expected_files, expected_size = asyncio.run(summary())
    print(
        f"found {expected_files:_} file(s), ~{expected_size:_} Mb in total",
        file=sys.stderr,
    )


def parquet(
    session: Session,
    info: Iterable[ParquetData],
    root_path: Path,
    *,
    country_subdir: bool = True,
    overwrite: bool = False,
):
    """request file urls and download unique files

    :param session:
        Parquet downloads API session.
    :param info:
        requests by country|city/pollutant.
    :param root_path:
        The directory to save files in (must exist).
    :param country_subdir: (optional, default `True`)
        Download files for different counties to different `root_path` sub directories.
        If False, download all files to `root_path`
    :param overwrite: (optional, default `False`)
        Re-download existing files in `root_path`.
        If False, existing files will be skipped.
        Empty files will be re-downloaded regardless of this option.
    """

    async def download():
        async with session:
            await session.url_to_files(*info)
            if session.number_of_urls == 0:
                hint = "please try different countries|cites/pollutants"
                warn(
                    f"Found no data matching your selection, {hint}",
                    UserWarning,
                )
                return

            await session.download_to_directory(
                root_path,
                country_subdir=country_subdir,
                skip_existing=not overwrite,
            )

    asyncio.run(download())
