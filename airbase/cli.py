from __future__ import annotations

import asyncio
from enum import Enum
from pathlib import Path
from typing import List, Optional

import typer

from . import __version__
from .parquet_api import Dataset, download
from .summary import DB

main = typer.Typer(add_completion=False, no_args_is_help=True)


class Country(str, Enum):
    _ignore_ = "country Country"  # type:ignore[misc]

    Country = vars()
    for country in sorted(DB.countries()):
        Country[country] = country

    def __str__(self) -> str:
        return self.name


class Pollutant(str, Enum):
    _ignore_ = "poll Pollutant"  # type:ignore[misc]

    Pollutant = vars()
    for poll in sorted(DB.pollutants(), key=lambda poll: len(poll)):
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
METADATA = typer.Option(
    False, "-M", "--metadata", help="download station metadata"
)
SUMMARY = typer.Option(
    False,
    "-n",
    "--dry-run",
    "--summary",
    help="Total download files/size, nothing will be downloaded.",
)
OVERWRITE = typer.Option(
    False, "-O", "--overwrite", help="Re-download existing files."
)
QUIET = typer.Option(False, "-q", "--quiet", help="No progress-bar.")


@main.command(no_args_is_help=True)
def historical(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data/historical", "--path", exists=True, dir_okay=True, writable=True
    ),
    summary_only: bool = SUMMARY,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Historical Airbase data delivered between 2002 and 2012 before Air Quality Directive 2008/50/EC entered into force.
    """
    asyncio.run(
        download(
            Dataset.Historical,
            path,
            countries=set(map(str, countries)),
            pollutants=set(map(str, pollutants)),
            cities=set(cities),
            metadata=metadata,
            summary_only=summary_only,
            overwrite=overwrite,
            quiet=quiet,
        )
    )


@main.command(no_args_is_help=True)
def verified(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data/verified", "--path", exists=True, dir_okay=True, writable=True
    ),
    summary_only: bool = SUMMARY,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Verified data (E1a) from 2013 to 2023 reported by countries by 30 September each year for the previous year.
    """
    asyncio.run(
        download(
            Dataset.Verified,
            path,
            countries=set(map(str, countries)),
            pollutants=set(map(str, pollutants)),
            cities=set(cities),
            metadata=metadata,
            summary_only=summary_only,
            overwrite=overwrite,
            quiet=quiet,
        )
    )


@main.command(no_args_is_help=True)
def unverified(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data/unverified", "--path", exists=True, dir_okay=True, writable=True
    ),
    summary_only: bool = SUMMARY,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2024.
    """
    asyncio.run(
        download(
            Dataset.Verified,
            path,
            countries=set(map(str, countries)),
            pollutants=set(map(str, pollutants)),
            cities=set(cities),
            metadata=metadata,
            summary_only=summary_only,
            overwrite=overwrite,
            quiet=quiet,
        )
    )
