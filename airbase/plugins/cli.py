from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

if TYPE_CHECKING:
    from airbase.cli import CtxObj


def catalog(
    ctx: typer.Context,
    *,
    path: Annotated[
        Path | None,
        typer.Option(
            "--path",
            "--data-path",
            dir_okay=True,
            readable=True,
            metavar="DATA",
            help="Override catalog search path.",
        ),
    ] = None,
    catalog: Annotated[
        Path | None,
        typer.Option(
            file_okay=True,
            writable=True,
            metavar="[DATA|PATH]/catalog.parquet",
            help="Override combined metadata path",
        ),
    ] = None,
    metadata: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            file_okay=True,
            readable=True,
            metavar="PATH/metadata.csv",
            help="Override station metadata path.",
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
        metadata = obj["path"] / "metadata.csv"
    if path is None:
        path = obj["path"]
    if catalog is None:
        catalog = path / "catalog.parquet"

    typer.echo(ctx.command_path)
    write_catalog(
        catalog,
        path,
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
