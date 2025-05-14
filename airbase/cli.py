import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, TypeAlias

if sys.version_info >= (3, 11):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


import typer
from click import Choice

from . import __version__
from .parquet_api import Dataset, Session, download, request_info
from .summary import DB

main = typer.Typer(name=__package__, add_completion=False)


class CtxObj(TypedDict):
    path: Path
    subdir: bool
    overwrite: bool
    quiet: bool
    donwloader: Callable[[Dataset, Path | None], None]


def print_version(value: bool):
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.callback(no_args_is_help=True, chain=True)
def callback(
    ctx: typer.Context,
    version: Annotated[
        bool,
        typer.Option("--version", "-V", callback=print_version),
    ] = False,
    countries: Annotated[
        list[str],
        typer.Option(
            "-c", "--country", click_type=Choice(sorted(DB.COUNTRY_CODES))
        ),
    ] = [],
    pollutants: Annotated[
        list[str],
        typer.Option(
            "-p",
            "--pollutant",
            click_type=Choice(
                sorted(DB.POLLUTANTS, key=lambda poll: len(poll))
            ),
        ),
    ] = [],
    cities: Annotated[
        list[str],
        typer.Option(
            "-C",
            "--city",
            help="Only from selected <cities> (--country option will be ignored).",
        ),
    ] = [],
    root_path: Annotated[
        Path,
        typer.Option(
            "--path",
            "--root-path",
            exists=True,
            dir_okay=True,
            writable=True,
            help="Donwload root path.",
        ),
    ] = Path("data"),
    subdir: Annotated[
        bool,
        typer.Option(
            "--subdir/--no-subdir",
            help="Download observations to PATH/dataset/[frequnecy/]country.",
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
    flush_stderr: Annotated[bool, typer.Option(hidden=True)] = False,
):
    """
    Download Air Quality Data from the European Environment Agency (EEA)

    \b
    Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
    or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
    - download only Norwegian, Danish and Finish sites Historical Airbase dataset (2002 to 2012)
      $ airbase -c NO -c DK -c FI historical
    - download only SO2, PM10 and PM2.5 observations from the Verified E1a dataset (2013 to 2024)
      $ airbase -p SO2 -p PM10 -p PM2.5 verified
    - download only PM10 and PM2.5 from sites in Oslo from the Unverified E2a dataset (from 2025)
      $ airbase -C Oslo -p PM10 -p PM2.5 unverified

    \b
    Chain commands to request data from different datasets
    - request an estimate of the number of files and disk size required to download all
      available observations
      $ airbase --summary --quiet historical verified unverified
    - download verified and unverified PM10 and PM2.5 observations from sites in Berlin
      $ airbase -C Berlin -p PM10 verified unverified
    """
    session = Session(progress=not quiet, raise_for_status=False)
    if not summary_only and metadata:
        asyncio.run(
            download(
                "METADATA",
                session,
                frozenset(),
                root_path / "metadata.csv",
                country_subdir=subdir,
                overwrite=overwrite,
            )
        )

    def donwloader(dataset: Dataset, path: Path | None):
        if path is None and subdir:  # default
            path = root_path.joinpath(dataset.name.casefold())
        if path is None:
            path = root_path
        if not summary_only:
            path.mkdir(parents=True, exist_ok=True)

        info = request_info(
            dataset, countries=countries, pollutants=pollutants, cities=cities
        )

        asyncio.run(
            download(
                "SUMMARY" if summary_only else "PARQUET",
                session,
                info,
                path,
                country_subdir=subdir,
                overwrite=overwrite,
            )
        )
        if flush_stderr:
            sys.stderr.flush()

    ctx.obj = CtxObj(
        path=root_path,
        subdir=subdir,
        overwrite=overwrite,
        quiet=quiet,
        donwloader=donwloader,
    )


def check_path(ctx: typer.Context, value: Path | None):
    if not isinstance(value, Path):
        return value

    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    if (path := obj.get("path")) is None:
        return value

    if not value.is_relative_to(path):
        raise typer.BadParameter(f"Is not subdir of `--path={path}/`")

    return value


PathOption: TypeAlias = Annotated[
    Path | None,
    typer.Option(
        "--path",
        "--data-path",
        dir_okay=True,
        writable=True,
        metavar="PATH/dataset/",
        help="Override dataset donwload path.",
        callback=check_path,
    ),
]


@main.command()
def historical(
    ctx: typer.Context,
    path: PathOption = None,
):
    """
    Historical Airbase data delivered between 2002 and 2012 before Air Quality Directive 2008/50/EC entered into force.
    """
    typer.echo(ctx.command_path)
    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    obj["donwloader"](Dataset.Historical, path)


@main.command()
def verified(
    ctx: typer.Context,
    path: PathOption = None,
):
    """
    Verified data (E1a) from 2013 to 2024 reported by countries by 30 September each year for the previous year.
    """
    typer.echo(ctx.command_path)
    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    obj["donwloader"](Dataset.Verified, path)


@main.command()
def unverified(
    ctx: typer.Context,
    path: PathOption = None,
):
    """
    Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the beginning of 2025.
    """
    typer.echo(ctx.command_path)
    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    obj["donwloader"](Dataset.Unverified, path)
