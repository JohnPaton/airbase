from pathlib import Path
from typing import Annotated

import typer

from airbase.cli import CtxObj, check_subdir


def catalog(
    ctx: typer.Context,
    *,
    subdir: Annotated[
        Path | None,
        typer.Option(
            "--subdir",
            file_okay=False,
            metavar="SUBDIR",
            help="Restrict catalog search path to PATH/SUBDIR.",
            callback=check_subdir,
        ),
    ] = None,
    catalog: Annotated[
        Path | None,
        typer.Option(
            dir_okay=False,
            writable=True,
            metavar="PARQUET",
            help="Override combined metadata path. Defaults to PATH/SUBDIR/catalog.parquet.",
        ),
    ] = None,
    metadata: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            metavar="CSV",
            help="Override station metadata path. Defaults to PATH/metadata.csv.",
        ),
    ] = None,
    stop_after: Annotated[
        int | None,
        typer.Option(
            "-N",
            "--stop-after",
            hidden=True,
            help="Only process <INTEGER> files",
        ),
    ] = None,
):
    """
    Combine station metadata with observation metadata from files found
    on the donwload root path (set by `airbase --path PATH`).
    """
    try:
        from .catalog import write_catalog
    except ModuleNotFoundError as e:  # pragma: no cover
        typer.echo(e)
        typer.echo(missing_extras(ctx.command_path, "catalog"))  # type:ignore[unreachable]
        raise typer.Abort()

    obj: CtxObj = ctx.ensure_object(dict)  # type:ignore[assignment]
    if metadata is None:
        metadata = obj["path"].joinpath("metadata.csv")
    if catalog is None:
        catalog = obj["path"].joinpath(subdir or "", "catalog.parquet")

    typer.echo(ctx.command_path)
    write_catalog(
        catalog,
        catalog.parent,
        metadata,
        overwrite=obj["overwrite"],
        stop_after=stop_after,
    )


def missing_extras(
    sub_cmd: str, *extras: str, package: str = "airbase"
) -> str:  # pragma: no cover
    from functools import partial
    from textwrap import dedent

    green = partial(typer.style, fg=typer.colors.GREEN)
    red = partial(typer.style, fg=typer.colors.RED)
    package = green(package, bold=True)
    extra = ",".join(red(extra, bold=True) for extra in extras)
    msg = f"""
        {green(sub_cmd, bold=True)} require dependencies which are not installed.

        You can install the required dependencies with
            {green("python3 -m pip install --upgrade")} {package}[{extra}]

        Or, if you installed {package} with {green("pipx")}
            {green("pipx install --force")} {package}[{extra}]

        Or, if you installed {package} with {green("uv tool")}
            {green("uv tool install")} {package}[{extra}]
    """
    return dedent(msg)
