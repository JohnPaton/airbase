import asyncio
from pathlib import Path
from typing import Annotated, Literal, TypeAlias

import typer
from click import Choice

from . import __version__
from .parquet_api import Dataset, Session, download, request_info
from .summary import DB

main = typer.Typer(add_completion=False, no_args_is_help=True)


def print_version(value: bool):
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.callback()
def callback(
    ctx:typer.Context,
    version: Annotated[
        bool,
        typer.Option("--version", "-V", callback=print_version),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option("-q", "--quiet", help="No progress-bar."),
    ] = False,
):
    """Download Air Quality Data from the European Environment Agency (EEA)"""

    obj = ctx.ensure_object(dict)
    obj.update(
        session=Session(progress=not quiet, raise_for_status=False),
        quiet=quiet,
    )


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
    typer.Option(
        "-C",
        "--city",
        help="Only from selected <cities> (--country option will be ignored).",
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


@main.command(no_args_is_help=True)
def historical(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    metadata: MetadataOption = False,
    path: PathOption = Path("data/historical"),
    summary_only: SummaryOption = False,
    country_subdir: SubdirOption = True,
    overwrite: OverwriteOption = False,
):
    """
    Historical Airbase data delivered between 2002 and 2012 before Air Quality Directive 2008/50/EC entered into force.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase historical -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase historical -p SO2 -p PM10 -p PM2.5
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase historical -C Valletta -p PM10 -p PM2.5
    """
    info = request_info(
        Dataset.Historical,
        countries=countries,
        pollutants=pollutants,
        cities=cities,
    )
    mode: Literal["SUMMARY", "METADATA", "PARQUET"]
    mode = "SUMMARY" if summary_only else "METADATA" if metadata else "PARQUET"
    obj = ctx.ensure_object(dict)
    asyncio.run(
        download(
            mode,
            obj["session"],
            info,
            path,
            country_subdir=country_subdir,
            overwrite=overwrite,
        )
    )


@main.command(no_args_is_help=True)
def verified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    metadata: MetadataOption = False,
    path: PathOption = Path("data/verified"),
    summary_only: SummaryOption = False,
    country_subdir: SubdirOption = True,
    overwrite: OverwriteOption = False,
):
    """
    Verified data (E1a) from 2013 to 2024 reported by countries by 30 September each year for the previous year.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase verified -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase verified -p SO2 -p PM10 -p PM2.5
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase verified -C Valletta -p PM10 -p PM2.5
    """
    info = request_info(
        Dataset.Verified,
        countries=countries,
        pollutants=pollutants,
        cities=cities,
    )
    mode: Literal["SUMMARY", "METADATA", "PARQUET"]
    mode = "SUMMARY" if summary_only else "METADATA" if metadata else "PARQUET"
    obj = ctx.ensure_object(dict)
    asyncio.run(
        download(
            mode,
            obj["session"],
            info,
            path,
            country_subdir=country_subdir,
            overwrite=overwrite,
        )
    )


@main.command(no_args_is_help=True)
def unverified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    metadata: MetadataOption = False,
    path: PathOption = Path("data/unverified"),
    summary_only: SummaryOption = False,
    country_subdir: SubdirOption = True,
    overwrite: OverwriteOption = False,
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2025.

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites
      airbase unverified -c NO -c DK -c FI
    - download only SO2, PM10 and PM2.5 observations
      airbase unverified -p SO2 -p PM10 -p PM2.5
    - download only PM10 and PM2.5 from Valletta, the Capital of Malta
      airbase unverified -C Valletta -p PM10 -p PM2.5
    """
    info = request_info(
        Dataset.Unverified,
        countries=countries,
        pollutants=pollutants,
        cities=cities,
    )
    obj = ctx.ensure_object(dict)
    mode: Literal["SUMMARY", "METADATA", "PARQUET"]
    mode = "SUMMARY" if summary_only else "METADATA" if metadata else "PARQUET"
    asyncio.run(
        download(
            mode,
            obj["session"],
            info,
            path,
            country_subdir=country_subdir,
            overwrite=overwrite,
        )
    )
