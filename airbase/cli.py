from __future__ import annotations

from datetime import date
from enum import Enum
from pathlib import Path
from typing import List

import typer

from . import __version__
from .airbase import AirbaseClient

main = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
)
client = AirbaseClient()


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
def root_options(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=version_callback,
        help=f"Show {__package__} version and exit.",
    ),
):
    """Download Air Quality Data from the European Environment Agency (EEA)"""


@main.command()
def download(
    countries: List[Country] = typer.Option([], "--country", "-c"),
    pollutants: List[Pollutant] = typer.Option([], "--pollutant", "-p"),
    path: Path = typer.Option(
        "data", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(date.today().year),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-O", help="Re-download existing files."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No progress-bar."),
):
    """Download all pollutants for all countries

    \b
    The -c/--country and -p/--pollutant allow to specify which data to download, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5
    """

    request = client.request(
        countries or None,  # type:ignore[arg-type]
        pollutants or None,  # type:ignore[arg-type]
        year_from=str(year),
        year_to=str(year),
        verbose=not quiet,
    )
    request.download_to_directory(path, skip_existing=not overwrite)


def deprecation_message(old: str, new: str):  # pragma: no cover
    old = typer.style(f"{__package__} {old}", fg=typer.colors.RED, bold=True)
    new = typer.style(f"{__package__} {new}", fg=typer.colors.GREEN, bold=True)
    typer.echo(
        f"{old} has been deprecated and will be removed on v1. Use {new} all instead.",
    )


@main.command(name="all")
def download_all(
    countries: List[Country] = typer.Option([], "--country", "-c"),
    pollutants: List[Pollutant] = typer.Option([], "--pollutant", "-p"),
    path: Path = typer.Option(
        "data", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(date.today().year),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-O", help="Re-download existing files."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No progress-bar."),
):  # pragma: no cover
    """Download all pollutants for all countries (deprecated)"""
    deprecation_message("all", "download")
    download(countries, pollutants, path, year, overwrite, quiet)


@main.command(name="country")
def download_country(
    country: Country,
    pollutants: List[Pollutant] = typer.Option([], "--pollutant", "-p"),
    path: Path = typer.Option(
        "data", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(date.today().year),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-O", help="Re-download existing files."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No progress-bar."),
):  # pragma: no cover
    """Download specific pollutants for one country (deprecated)"""
    deprecation_message("country", "download")
    download([country], pollutants, path, year, overwrite, quiet)


@main.command(name="pollutant")
def download_pollutant(
    pollutant: Pollutant,
    countries: List[Country] = typer.Option([], "--country", "-c"),
    path: Path = typer.Option(
        "data", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(date.today().year),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-O", help="Re-download existing files."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No progress-bar."),
):  # pragma: no cover
    """Download specific countries for one pollutant (deprecated)"""
    deprecation_message("pollutant", "download")
    download(countries, [pollutant], path, year, overwrite, quiet)
