from __future__ import annotations

from datetime import date
from enum import Enum
from pathlib import Path
from typing import List, Optional

import typer

from . import __version__
from .airbase import AirbaseClient

client = AirbaseClient()
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
    """Download all pollutants for all countries

    \b
    The -c/--country and -p/--pollutant allow to specify which data to download, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5
    """
    _download(countries, pollutants, path, year, overwrite, quiet)


def deprecation_message(old: str, new: str):  # pragma: no cover
    old = typer.style(f"{__package__} {old}", fg="red", bold=True)
    new = typer.style(f"{__package__} {new}", fg="green", bold=True)
    typer.echo(
        f"{old} has been deprecated and will be removed on v1. Use {new} all instead.",
    )


@main.command(name="all", no_args_is_help=True)
def download_all(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    path: Path = PATH,
    year: int = YEAR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):  # pragma: no cover
    """Download all pollutants for all countries (deprecated)"""
    deprecation_message("all", "download")
    _download(countries, pollutants, path, year, overwrite, quiet)


@main.command(name="country", no_args_is_help=True)
def download_country(
    country: Country = typer.Argument(),
    pollutants: List[Pollutant] = POLLUTANTS,
    path: Path = PATH,
    year: int = YEAR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):  # pragma: no cover
    """Download specific pollutants for one country (deprecated)"""
    deprecation_message("country", "download")
    _download([country], pollutants, path, year, overwrite, quiet)


@main.command(name="pollutant", no_args_is_help=True)
def download_pollutant(
    pollutant: Pollutant = typer.Argument(),
    countries: List[Country] = COUNTRIES,
    path: Path = PATH,
    year: int = YEAR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):  # pragma: no cover
    """Download specific countries for one pollutant (deprecated)"""
    deprecation_message("pollutant", "download")
    _download(countries, [pollutant], path, year, overwrite, quiet)
