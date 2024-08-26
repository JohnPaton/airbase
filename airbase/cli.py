from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import date
from enum import Enum
from itertools import product
from pathlib import Path
from typing import List, Optional

import typer

from . import __version__
from .airbase import AirbaseClient
from .download_api import DownloadClient, DownloadInfo

client = AirbaseClient()
new_client = DownloadClient()
main = typer.Typer(add_completion=False, no_args_is_help=True)


class Country(str, Enum):
    _ignore_ = "country Country"  # type:ignore[misc]

    Country = vars()
    for country in client.countries:
        Country[country] = country

    def __str__(self) -> str:
        return self.name


class Pollutant(str, Enum):
    _ignore_ = "poll Pollutant"  # type:ignore[misc]

    Pollutant = vars()
    for poll in client._pollutants_ids:
        Pollutant[poll] = poll

    def __str__(self) -> str:
        return self.name


def version_callback(value: bool):
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.callback()
def callback(
    version: Optional[bool] = typer.Option(
        None, "--version", "-V", callback=version_callback
    ),
):
    """Download Air Quality Data from the European Environment Agency (EEA)"""


COUNTRIES = typer.Option([], "-c", "--country")
POLLUTANTS = typer.Option([], "-p", "--pollutant")
CITIES = typer.Option([], "-C", "--city", help="only from selected <cities>")
PATH = typer.Option("data", "--path", exists=True, dir_okay=True, writable=True)
YEAR = typer.Option(date.today().year, "--year")
OVERWRITE = typer.Option(
    False, "-O", "--overwrite", help="Re-download existing files."
)
QUIET = typer.Option(False, "-q", "--quiet", help="No progress-bar.")


def _download(
    countries: list[Country],
    pollutants: list[Pollutant],
    path: Path,
    year: int,
    overwrite: bool,
    quiet: bool,
):
    request = client.request(
        countries or None,  # type:ignore[arg-type]
        pollutants or None,  # type:ignore[arg-type]
        year_from=str(year),
        year_to=str(year),
        verbose=not quiet,
    )
    request.download_to_directory(path, skip_existing=not overwrite)


@main.command(no_args_is_help=True)
def download(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    path: Path = PATH,
    year: int = YEAR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """Download all pollutants for all countries (discontinued, EOL end of 2024)

    \b
    The -c/--country and -p/--pollutant allow to specify which data to download, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5
    """
    eol_message("download", "historical", "verified", "unverified")
    _download(countries, pollutants, path, year, overwrite, quiet)


def eol_message(old: str, *new: str):  # pragma: no cover
    old = typer.style(f"{__package__} {old}", fg="red", bold=True)
    new = tuple(
        typer.style(f"{__package__} {n}", fg="green", bold=True) for n in new
    )
    typer.echo(
        f"The service behind {old} has been discontinued and will stop working by the end of 2024. Use {', '.join(new)} all instead.",
    )


async def _new_download(
    info: Iterator[DownloadInfo],
    *,
    path: Path,
    overwrite: bool,
    quiet: bool,
):
    async with new_client as session:
        urls = await session.url_to_files(*info, progress=not quiet)
        await session.download_to_directory(
            path, *urls, skip_existing=not overwrite, progress=not quiet
        )


@main.command(no_args_is_help=True)
def historical(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    path: Path = typer.Option(
        "data/historical", "--path", exists=True, dir_okay=True, writable=True
    ),
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Historical Airbase data delivered between 2002 and 2012 before Air Quality Directive 2008/50/EC entered into force.
    """
    info = (
        DownloadInfo.historical(pollutant, country, *cities)
        for pollutant, country in product(pollutants, countries)
    )
    asyncio.run(
        _new_download(info, path=path, overwrite=overwrite, quiet=quiet)
    )


@main.command(no_args_is_help=True)
def verified(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    path: Path = typer.Option(
        "data/verified", "--path", exists=True, dir_okay=True, writable=True
    ),
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Verified data (E1a) from 2013 to 2022 reported by countries by 30 September each year for the previous year.
    """
    info = (
        DownloadInfo.verified(pollutant, country, *cities)
        for pollutant, country in product(pollutants, countries)
    )
    asyncio.run(
        _new_download(info, path=path, overwrite=overwrite, quiet=quiet)
    )


@main.command(no_args_is_help=True)
def unverified(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    path: Path = typer.Option(
        "data/unverified", "--path", exists=True, dir_okay=True, writable=True
    ),
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2023.
    """
    info = (
        DownloadInfo.unverified(pollutant, country, *cities)
        for pollutant, country in product(pollutants, countries)
    )
    asyncio.run(
        _new_download(info, path=path, overwrite=overwrite, quiet=quiet)
    )
