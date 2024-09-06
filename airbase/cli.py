from __future__ import annotations

import asyncio
from enum import Enum
from pathlib import Path
from typing import List, Optional

import typer

from . import __version__
from .csv_api import Source
from .csv_api import download as download_csv
from .parquet_api import Dataset
from .parquet_api import download as download_parquet
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


@main.command("download", no_args_is_help=True)
def legacy(
    countries: List[Country] = COUNTRIES,
    pollutants: List[Pollutant] = POLLUTANTS,
    cities: List[str] = CITIES,
    metadata: bool = METADATA,
    path: Path = typer.Option(
        "data", "--path", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(
        2024,
        "--year",
        min=2024,
        max=2024,
        help="""\b
        The service providing air quality data in CSV format will cease operations by the end of 2024.
        Until then it will provide only **unverified** data (E2a) for 2024.
        """,
    ),
    overwrite: bool = OVERWRITE,
    quiet: bool = QUIET,
):
    """
    Air quality data in in CSV format. **End of life 2024**.

    \b
    The service providing air quality data in CSV format will cease operations by the end of 2024.
    Until then it will provide only **unverified** data (E2a) for 2024.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase download -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Source.ALL,
            path,
            year=year,
            countries=countries,
            pollutants=pollutants,
            cities=cities,
            metadata=metadata,
            overwrite=overwrite,
            quiet=quiet,
        )
    )


async def download(
    dataset: Dataset | Source,
    path: Path,
    *,
    countries: list[Country],
    pollutants: list[Pollutant],
    cities: list[str],
    metadata: bool,
    overwrite: bool,
    quiet: bool,
    summary_only: bool | None = None,
    year: int | None = None,
) -> None:
    """download CSV or Parquet files from corresponding API"""
    if isinstance(dataset, Dataset):
        if summary_only is None:
            raise typer.BadParameter("missing --dry-run/--summary option")

        await download_parquet(
            dataset,
            path,
            countries=frozenset(map(str, countries)),
            pollutants=frozenset(map(str, pollutants)),
            cities=frozenset(cities),
            metadata=metadata,
            summary_only=summary_only,
            overwrite=overwrite,
            quiet=quiet,
        )
        return

    if isinstance(dataset, Source):
        if year is None:
            raise typer.BadParameter("missing --year option")

        await download_csv(
            dataset,
            year,
            path,
            countries=frozenset(map(str, countries)),
            pollutants=frozenset(map(str, pollutants)),
            cities=frozenset(cities),
            metadata=metadata,
            overwrite=overwrite,
            quiet=quiet,
        )
        return

    # should never reach
    raise ValueError(
        f"Unsupported dataset, summary, year: {dataset}, {summary_only}, {year}."
    )


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

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase download -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Dataset.Historical,
            path,
            countries=countries,
            pollutants=pollutants,
            cities=cities,
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

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase download -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Dataset.Verified,
            path,
            countries=countries,
            pollutants=pollutants,
            cities=cities,
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

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase download -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase download -p SO2 -p PM10 -p PM2.5

    \b
    Use -C/--city to further restrict the download to specific cities, e.g.
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase download -C Valletta -c MT -p PM10 -p PM2.5
    """
    asyncio.run(
        download(
            Dataset.Verified,
            path,
            countries=countries,
            pollutants=pollutants,
            cities=cities,
            metadata=metadata,
            summary_only=summary_only,
            overwrite=overwrite,
            quiet=quiet,
        )
    )
