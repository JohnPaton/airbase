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

main = typer.Typer(add_completion=False, no_args_is_help=True, chain=True)


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
    name: str
    info: frozenset[ParquetData]
    path: Path
    metadata: bool = False


@contextmanager
def downloader(
    session: Session,
    *,
    summary_only: bool,
    country_subdir: bool,
    overwrite: bool,
):
    async def download_(reqests: set[Request]):
        for req in reqests:
            typer.echo(req.name)
            await download(
                session,
                req.info,
                req.path,
                metadata_only=req.metadata,
                summary_only=summary_only,
                country_subdir=country_subdir,
                overwrite=overwrite,
            )

    reqests: set[Request] = set()
    yield reqests
    asyncio.run(download_(reqests))


def print_version(value: bool):
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.callback()
def callback(
    ctx: typer.Context,
    version: Annotated[
        Optional[bool],
        typer.Option("--version", "-V", callback=print_version),
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
    """
    Download Air Quality Data from the European Environment Agency (EEA)

    \b
    Use -n/--dry-run/--summary and -q/--quiet to request the number of files and
    estimated download size without downloading the observations, e.g
    - total download files/size for hourly verified and unverified observations
      airbase --quiet --summary \\
        verified -F hourly \\
        unverified -F hourly

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download verified hourly and daily PM10 and PM2.5 observations from sites in Oslo
      to different (existing) paths in order to avoid filename collisions
      airbase --no-subdir \\
        verified -p PM10 -p PM2.5 -C Oslo -F daily  --path data/daily \\
        verified -p PM10 -p PM2.5 -C Oslo -F hourly --path data/hourly
    """

    ctx.obj = ctx.with_resource(
        downloader(
            Session(progress=not quiet, raise_for_status=False),
            summary_only=summary_only,
            country_subdir=country_subdir,
            overwrite=overwrite,
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
    - download only PM10 and PM2.5 observations from sites in Oslo
      airbase historical -p PM10 -p PM2.5 -C Oslo
    """
    info = request_info(
        Dataset.Historical,
        countries=set(map(str, countries)),
        pollutants=set(map(str, pollutants)),
        cities=set(cities),
        frequency=None if frequency is None else frequency.aggregation_type,
    )
    name = f"{ctx.command_path} {frequency}" if frequency else ctx.command_path
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(name, frozenset(info), path))


@main.command(no_args_is_help=True)
def verified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
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
    - download only PM10 and PM2.5 observations from sites in Oslo
      airbase verified -p PM10 -p PM2.5 -C Oslo
    """
    info = request_info(
        Dataset.Verified,
        countries=set(map(str, countries)),
        pollutants=set(map(str, pollutants)),
        cities=set(cities),
        frequency=None if frequency is None else frequency.aggregation_type,
    )
    name = f"{ctx.command_path} {frequency}" if frequency else ctx.command_path
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(name, frozenset(info), path))


@main.command(no_args_is_help=True)
def unverified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    frequency: FrequencyOption = None,
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
    - download only PM10 and PM2.5 observations from sites in Oslo
      airbase unverified -p PM10 -p PM2.5 -C Oslo
    """
    info = request_info(
        Dataset.Unverified,
        countries=set(map(str, countries)),
        pollutants=set(map(str, pollutants)),
        cities=set(cities),
        frequency=None if frequency is None else frequency.aggregation_type,
    )
    name = f"{ctx.command_path} {frequency}" if frequency else ctx.command_path
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(name, frozenset(info), path))


@main.command(no_args_is_help=True)
def metadata(
    ctx: typer.Context,
    path: Annotated[
        Path,
        typer.Argument(file_okay=True, dir_okay=True, writable=True),
    ],
):
    """Download station metadata."""
    obj: set[Request] = ctx.ensure_object(set)
    obj.add(Request(ctx.command_path, frozenset(), path, True))
