from __future__ import annotations

import asyncio
from enum import Enum
from pathlib import Path
from typing import List, Optional

import typer

from . import __version__
from .parquet_api import AggregationType, Dataset, download
from .summary import DB

main = typer.Typer(add_completion=False, no_args_is_help=True)


class Country(str, Enum):
    _ignore_ = "country Country"  # type:ignore[misc]

    Country = vars()
    for country in sorted(DB.COUNTRY_CODES):
        Country[country] = country

    def __str__(self) -> str:
        return self.name


class Pollutant(str, Enum):
    _ignore_ = "poll Pollutant"  # type:ignore[misc]

    Pollutant = vars()
    for poll in sorted(DB.POLLUTANTS, key=lambda poll: len(poll)):
        Pollutant[poll] = poll

    def __str__(self) -> str:
        return self.name


class Frequency(str, Enum):
    _ignore_ = "agg Frequency"  # type:ignore[misc]

    Frequency = vars()
    for agg in AggregationType:
        Frequency[agg.name.casefold()] = agg.name.casefold()

    def __str__(self) -> str:
        return self.name

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType[self.name.capitalize()]


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
CITIES = typer.Option([], "-C", "--city", help="Only from selected <cities>.")
FREQUENCY = typer.Option(
    None,
    "--aggregation-type",
    "--frequency",
    "-F",
    help="Only hourly data, daily data or other aggregation frequency.",
)
METADATA = typer.Option(
    False, "-M", "--metadata", help="Download station metadata."
)
SUMMARY = typer.Option(
    False,
    "-n",
    "--dry-run",
    "--summary",
    help="Total download files/size, nothing will be downloaded.",
)
FLAT_DIR = typer.Option(
    False,
    "--subdir/--no-subdir",
    help="Download files for different counties to different sub directories.",
)
COUNTRY_SUBDIR = typer.Option(
    True,
    "--subdir/--no-subdir",
    help="Download files for different counties to different sub directories.",
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
    frequency: Optional[Frequency] = FREQUENCY,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data/historical", "--path", exists=True, dir_okay=True, writable=True
    ),
    summary_only: bool = SUMMARY,
    country_subdir: bool = COUNTRY_SUBDIR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Historical Airbase data delivered between 2002 and 2012 before Air Quality Directive 2008/50/EC entered into force.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase historical -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase historical -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase historical -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Dataset.Historical,
            path,
            countries=frozenset(map(str, countries)),
            pollutants=frozenset(map(str, pollutants)),
            cities=frozenset(cities),
            frequency=None if frequency is None else frequency.aggregation_type,
            metadata=metadata,
            summary_only=summary_only,
            country_subdir=country_subdir,
            overwrite=overwrite,
            quiet=quiet,
        )
    )


@main.command(no_args_is_help=True)
def verified(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    frequency: Optional[Frequency] = FREQUENCY,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data/verified", "--path", exists=True, dir_okay=True, writable=True
    ),
    summary_only: bool = SUMMARY,
    country_subdir: bool = COUNTRY_SUBDIR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Verified data (E1a) from 2013 to 2023 reported by countries by 30 September each year for the previous year.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase verified -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase verified -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase verified -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Dataset.Verified,
            path,
            countries=frozenset(map(str, countries)),
            pollutants=frozenset(map(str, pollutants)),
            cities=frozenset(cities),
            frequency=None if frequency is None else frequency.aggregation_type,
            metadata=metadata,
            summary_only=summary_only,
            country_subdir=country_subdir,
            overwrite=overwrite,
            quiet=quiet,
        )
    )


@main.command(no_args_is_help=True)
def unverified(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    frequency: Optional[Frequency] = FREQUENCY,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data/unverified", "--path", exists=True, dir_okay=True, writable=True
    ),
    summary_only: bool = SUMMARY,
    country_subdir: bool = COUNTRY_SUBDIR,
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2024.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase unverified -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase unverified -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase unverified -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Dataset.Unverified,
            path,
            countries=frozenset(map(str, countries)),
            pollutants=frozenset(map(str, pollutants)),
            cities=frozenset(cities),
            frequency=None if frequency is None else frequency.aggregation_type,
            metadata=metadata,
            summary_only=summary_only,
            country_subdir=country_subdir,
            overwrite=overwrite,
            quiet=quiet,
        )
    )
