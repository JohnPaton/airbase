import asyncio
import sys
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Annotated, NamedTuple, Optional

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias


import typer

from . import __version__
from .parquet_api import (
    AggregationType,
    Dataset,
    ParquetData,
    Session,
    download,
    request_info,
)
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


class Request(NamedTuple):
    info: frozenset[ParquetData]
    path: Path
    metadata: bool


@contextmanager
def downloader(
    *,
    summary_only: bool,
    country_subdir: bool,
    overwrite: bool,
    session: Session,
):
    reqests: set[Request] = set()
    yield reqests
    for req in reqests:
        asyncio.run(
            download(
                req.info,
                req.path,
                metadata=req.metadata,
                summary_only=summary_only,
                country_subdir=country_subdir,
                overwrite=overwrite,
                session=session,
            )
        )


def version_callback(value: bool):
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.callback()
def callback(
    ctx: typer.Context,
    version: Annotated[
        Optional[bool],
        typer.Option("--version", "-V", callback=version_callback),
    ] = None,
    summary_only: Annotated[
        bool,
        typer.Option(
            "-n",
            "--dry-run",
            "--summary",
            help="Total download files/size, nothing will be downloaded.",
        ),
    ] = False,
    country_subdir: Annotated[
        bool,
        typer.Option(
            "--subdir/--no-subdir",
            help="Download files for different counties to different sub directories.",
        ),
    ] = True,
    overwrite: Annotated[
        bool,
        typer.Option("-O", "--overwrite", help="Re-download existing files."),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option("-q", "--quiet", help="No progress-bar."),
    ] = False,
):
    """Download Air Quality Data from the European Environment Agency (EEA)"""

    ctx.obj = ctx.with_resource(
        downloader(
            summary_only=summary_only,
            country_subdir=country_subdir,
            overwrite=overwrite,
            session=Session(progress=not quiet, raise_for_status=False),
        )
    )


CountryList: TypeAlias = Annotated[
    list[Country],
    typer.Option("-c", "--country"),
]
PollutantList: TypeAlias = Annotated[
    list[Pollutant],
    typer.Option("-p", "--pollutant"),
]
CityList: TypeAlias = Annotated[
    list[str],
    typer.Option(
        "-C",
        "--city",
        help="Only from selected <cities> (--country option will be ignored).",
    ),
]
FrequencyOption: TypeAlias = Annotated[
    Optional[Frequency],
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


@main.command(no_args_is_help=True)
def historical(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
    metadata: MetadataOption = False,
    path: PathOption = Path("data/historical"),
) -> None:
    """
    Historical Airbase data delivered between 2002 and 2012 before Air Quality Directive 2008/50/EC entered into force.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only from Norwegian, Danish and Finish sites
      airbase historical -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase historical -p SO2 -p PM10 -p PM2.5
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase historical -C Valletta -p PM10 -p PM2.5
    """
    info = request_info(
        Dataset.Historical,
        countries=set(map(str, countries)),
        pollutants=set(map(str, pollutants)),
        cities=set(cities),
        frequency=None if frequency is None else frequency.aggregation_type,
    )
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(frozenset(info), path, metadata))


@main.command(no_args_is_help=True)
def verified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
    metadata: MetadataOption = False,
    path: PathOption = Path("data/verified"),
):
    """
    Verified data (E1a) from 2013 to 2023 reported by countries by 30 September each year for the previous year.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only from Norwegian, Danish and Finish sites
      airbase verified -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase verified -p SO2 -p PM10 -p PM2.5
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase verified -C Valletta -p PM10 -p PM2.5
    """
    info = request_info(
        Dataset.Verified,
        countries=set(map(str, countries)),
        pollutants=set(map(str, pollutants)),
        cities=set(cities),
        frequency=None if frequency is None else frequency.aggregation_type,
    )
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(frozenset(info), path, metadata))


@main.command(no_args_is_help=True)
def unverified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
    metadata: MetadataOption = False,
    path: PathOption = Path("data/unverified"),
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2024.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only from Norwegian, Danish and Finish sites
      airbase unverified -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase unverified -p SO2 -p PM10 -p PM2.5
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase unverified -C Valletta -p PM10 -p PM2.5
    """
    info = request_info(
        Dataset.Unverified,
        countries=set(map(str, countries)),
        pollutants=set(map(str, pollutants)),
        cities=set(cities),
        frequency=None if frequency is None else frequency.aggregation_type,
    )
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(frozenset(info), path, metadata))
