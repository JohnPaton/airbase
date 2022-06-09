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
    for country in client.all_countries:
        Country[country] = country


class Pollutant(str, Enum):
    _ignore_ = "poll Pollutant"  # type:ignore[misc]

    Pollutant = vars()
    for poll in client.all_pollutants:
        Pollutant[poll] = poll


def version_callback(value: bool):  # pragma: no cover
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


@main.command(name="all")
def download_all(
    path: Path = typer.Option(
        "data", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(date.today().year),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-O", help="Re-download existing files."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No progress-bar."),
):
    """Download all pollutants for all countries"""

    request = client.request(
        year_from=str(year),
        year_to=str(year),
        verbose=not quiet,
    )
    request.download_to_directory(path, skip_existing=not overwrite)


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
):
    """Download specific pollutants for one country"""
    request = client.request(
        country,
        pollutants or None,  # type:ignore[arg-type]
        year_from=str(year),
        year_to=str(year),
        verbose=not quiet,
    )
    request.download_to_directory(path, skip_existing=not overwrite)


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
):
    """Download specific countries for one pollutant"""
    request = client.request(
        countries or None,  # type:ignore[arg-type]
        pollutant,
        year_from=str(year),
        year_to=str(year),
        verbose=not quiet,
    )
    request.download_to_directory(path, skip_existing=not overwrite)
