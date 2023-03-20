from __future__ import annotations

from datetime import date
from enum import Enum
from pathlib import Path

import click
import typer

from . import __version__
from .airbase import AirbaseClient

app = typer.Typer(no_args_is_help=True, add_completion=False)
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


@app.callback()
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


countries = click.option(
    "-c",
    "--country",
    "countries",
    type=click.Choice(Country),  # type:ignore [arg-type]
    multiple=True,
)
country = click.argument(
    "country",
    type=click.Choice(Country),  # type:ignore [arg-type]
)

pollutants = click.option(
    "-p",
    "--pollutant",
    "pollutants",
    type=click.Choice(Pollutant),  # type:ignore [arg-type]
    multiple=True,
)
pollutant = click.argument(
    "pollutant",
    type=click.Choice(Pollutant),  # type:ignore [arg-type]
)


path = click.option(
    "--path",
    default="data",
    type=click.Path(exists=True, dir_okay=True, writable=True),
)
year = click.option("--year", default=date.today().year, type=int)
overwrite = click.option(
    "-O",
    "--overwrite",
    is_flag=True,
    help="Re-download existing files.",
)
quiet = click.option(
    "-q",
    "--quiet",
    is_flag=True,
    help="No progress-bar.",
)


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


@click.command()
@countries
@pollutants
@path
@year
@overwrite
@quiet
def download(
    countries: list[Country],
    pollutants: list[Pollutant],
    path: Path,
    year: int,
    overwrite: bool,
    quiet: bool,
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
    old = typer.style(f"{__package__} {old}", fg=typer.colors.RED, bold=True)
    new = typer.style(f"{__package__} {new}", fg=typer.colors.GREEN, bold=True)
    typer.echo(
        f"{old} has been deprecated and will be removed on v1. Use {new} all instead.",
    )


@click.command()
@countries
@pollutants
@path
@year
@overwrite
@quiet
def download_all(
    countries: list[Country],
    pollutants: list[Pollutant],
    path: Path,
    year: int,
    overwrite: bool,
    quiet: bool,
):  # pragma: no cover
    """Download all pollutants for all countries (deprecated)"""
    deprecation_message("all", "download")
    _download(countries, pollutants, path, year, overwrite, quiet)


@click.command()
@country
@pollutants
@path
@year
@overwrite
@quiet
def download_country(
    country: Country,
    pollutants: list[Pollutant],
    path: Path,
    year: int,
    overwrite: bool,
    quiet: bool,
):  # pragma: no cover
    """Download specific pollutants for one country (deprecated)"""
    deprecation_message("country", "download")
    _download([country], pollutants, path, year, overwrite, quiet)


@click.command()
@pollutant
@countries
@path
@year
@overwrite
@quiet
def download_pollutant(
    pollutant: Pollutant,
    countries: list[Country],
    path: Path,
    year: int,
    overwrite: bool,
    quiet: bool,
):  # pragma: no cover
    """Download specific countries for one pollutant (deprecated)"""
    deprecation_message("pollutant", "download")
    _download(countries, [pollutant], path, year, overwrite, quiet)


# click object
main: click.Group = typer.main.get_command(app)  # type:ignore [assignment]
main.add_command(download, "download")
main.add_command(download_all, "all")
main.add_command(download_country, "country")
main.add_command(download_pollutant, "pollutant")
