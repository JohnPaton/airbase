import asyncio
from enum import Enum
from pathlib import Path
from typing import Annotated, TypeAlias

import typer
from click import Choice

from . import __version__
from .parquet_api import AggregationType, Dataset, download
from .summary import DB

main = typer.Typer(add_completion=False, no_args_is_help=True)


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
    version: Annotated[
        bool,
        typer.Option("--version", "-V", callback=version_callback),
    ] = False,
):
    """Download Air Quality Data from the European Environment Agency (EEA)"""


CountryList: TypeAlias = Annotated[
    list[str],
    typer.Option(
        "-c", "--country", click_type=Choice(sorted(DB.COUNTRY_CODES))
    ),
]
PollutantList: TypeAlias = Annotated[
    list[str],
    typer.Option(
        "-p",
        "--pollutant",
        click_type=Choice(sorted(DB.POLLUTANTS, key=lambda poll: len(poll))),
    ),
]
CityList: TypeAlias = Annotated[
    list[str],
    typer.Option("-C", "--city", help="Only from selected <cities>."),
]
FrequencyOption: TypeAlias = Annotated[
    Frequency | None,
    typer.Option(
        "--aggregation-type",
        "--frequency",
        "-F",
        help="Only hourly data, daily data or other aggregation frequency.",
    ),
]
MetadataOption: TypeAlias = Annotated[
    bool, typer.Option("-M", "--metadata", help="Download station metadata.")
]
PathOption: TypeAlias = Annotated[
    Path, typer.Option("--path", exists=True, dir_okay=True, writable=True)
]
SummaryOption: TypeAlias = Annotated[
    bool,
    typer.Option(
        "-n",
        "--dry-run",
        "--summary",
        help="Total download files/size, nothing will be downloaded.",
    ),
]
SubdirOption: TypeAlias = Annotated[
    bool,
    typer.Option(
        "--subdir/--no-subdir",
        help="Download files for different counties to different sub directories.",
    ),
]
OverwriteOption: TypeAlias = Annotated[
    bool,
    typer.Option("-O", "--overwrite", help="Re-download existing files."),
]
QuietOption: TypeAlias = Annotated[
    bool,
    typer.Option("-q", "--quiet", help="No progress-bar."),
]


@main.command(no_args_is_help=True)
def historical(
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
    metadata: MetadataOption = False,
    path: PathOption = Path("data/historical"),
    summary_only: SummaryOption = False,
    country_subdir: SubdirOption = True,
    overwrite: OverwriteOption = False,
    quiet: QuietOption = False,
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
            countries=frozenset(countries),
            pollutants=frozenset(pollutants),
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
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
    metadata: MetadataOption = False,
    path: PathOption = Path("data/verified"),
    summary_only: SummaryOption = False,
    country_subdir: SubdirOption = True,
    overwrite: OverwriteOption = False,
    quiet: QuietOption = False,
):
    """
    Verified data (E1a) from 2013 to 2024 reported by countries by 30 September each year for the previous year.

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
            countries=frozenset(countries),
            pollutants=frozenset(pollutants),
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
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
    metadata: MetadataOption = False,
    path: PathOption = Path("data/unverified"),
    summary_only: SummaryOption = False,
    country_subdir: SubdirOption = True,
    overwrite: OverwriteOption = False,
    quiet: QuietOption = False,
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2025.

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
            countries=frozenset(countries),
            pollutants=frozenset(pollutants),
            cities=frozenset(cities),
            frequency=None if frequency is None else frequency.aggregation_type,
            metadata=metadata,
            summary_only=summary_only,
            country_subdir=country_subdir,
            overwrite=overwrite,
            quiet=quiet,
        )
    )
