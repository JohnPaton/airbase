import asyncio
import sys
from pathlib import Path
from typing import Annotated, Literal, TypeAlias

if sys.version_info >= (3, 11):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


import typer
from click import Choice

from . import __version__
from .parquet_api import Dataset, Session, download, request_info
from .summary import DB

main = typer.Typer(name=__package__, add_completion=False, no_args_is_help=True)


class CtxObj(TypedDict):
    mode: Literal["SUMMARY", "PARQUET"]
    session: Session
    path: Path
    subdir: bool
    overwrite: bool
    quiet: bool


def print_version(value: bool):
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.callback()
def callback(
    ctx: typer.Context,
    version: Annotated[
        bool,
        typer.Option("--version", "-V", callback=print_version),
    ] = False,
    path: Annotated[
        Path, typer.Option(exists=True, dir_okay=True, writable=True)
    ] = Path("data"),
    subdir: Annotated[
        bool,
        typer.Option(
            "--subdir/--no-subdir",
            help="Download files for different counties to different sub directories.",
        ),
    ] = True,
    metadata: Annotated[
        bool,
        typer.Option(
            "-M",
            "--metadata",
            help="Download station metadata to PATH/metadata.csv.",
        ),
    ] = False,
    summary_only: Annotated[
        bool,
        typer.Option(
            "-n",
            "--dry-run",
            "--summary",
            help="Total download files/size, nothing will be downloaded.",
        ),
    ] = False,
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

    session = Session(progress=not quiet, raise_for_status=False)
    ctx.obj = CtxObj(
        mode="SUMMARY" if summary_only else "PARQUET",
        session=session,
        path=path,
        subdir=subdir,
        overwrite=overwrite,
        quiet=quiet,
    )

    if not summary_only and metadata:
        asyncio.run(
            download(
                "METADATA",
                session,
                frozenset(),
                path / "metadata.csv",
                country_subdir=subdir,
                overwrite=overwrite,
            )
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


def check_path(ctx: typer.Context, value: Path | None):
    if not isinstance(value, Path):
        return value

    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    if (path := obj.get("path")) is None:
        return value

    if not value.is_relative_to(path):
        raise typer.BadParameter(f"Is not subdir of `--path={path}/`")

    return value


@main.command(no_args_is_help=True)
def historical(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    path: Annotated[
        Path | None,
        typer.Option(
            "--data-path",
            exists=True,
            dir_okay=True,
            writable=True,
            metavar="PATH/dataset",
            help="[default: PATH/historical]",
            show_default=False,
            callback=check_path,
        ),
    ] = None,
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
    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    if path is None:
        assert ctx.info_name is not None
        path = obj["path"] / ctx.info_name
    asyncio.run(
        download(
            obj["mode"],
            obj["session"],
            info,
            path,
            country_subdir=obj["subdir"],
            overwrite=obj["overwrite"],
        )
    )


@main.command(no_args_is_help=True)
def verified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    path: Annotated[
        Path | None,
        typer.Option(
            "--data-path",
            exists=True,
            dir_okay=True,
            writable=True,
            metavar="PATH/dataset",
            help="[default: PATH/verified]",
            show_default=False,
            callback=check_path,
        ),
    ] = None,
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
    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    if path is None:
        assert ctx.info_name is not None
        path = obj["path"] / ctx.info_name
    asyncio.run(
        download(
            obj["mode"],
            obj["session"],
            info,
            path,
            country_subdir=obj["subdir"],
            overwrite=obj["overwrite"],
        )
    )


@main.command(no_args_is_help=True)
def unverified(
    ctx: typer.Context,
    countries: CountryList = [],
    pollutants: PollutantList = [],
    cities: CityList = [],
    path: Annotated[
        Path | None,
        typer.Option(
            "--data-path",
            exists=True,
            dir_okay=True,
            writable=True,
            metavar="PATH/dataset",
            help="[default: PATH/unverified]",
            show_default=False,
            callback=check_path,
        ),
    ] = None,
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
    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    if path is None:
        assert ctx.info_name is not None
        path = obj["path"] / ctx.info_name
    asyncio.run(
        download(
            obj["mode"],
            obj["session"],
            info,
            path,
            country_subdir=obj["subdir"],
            overwrite=obj["overwrite"],
        )
    )
