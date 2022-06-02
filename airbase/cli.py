from datetime import date
from pathlib import Path

import typer

from . import __version__
from .airbase import AirbaseClient

main = typer.Typer(add_completion=False)


def version_callback(value: bool):  # pragma: no cover
    if not value:
        return

    typer.echo(f"{__package__} v{__version__}")
    raise typer.Exit()


@main.command()
def download(
    path: Path = typer.Option(
        "data", exists=True, dir_okay=True, writable=True
    ),
    year: int = typer.Option(date.today().year),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-O", help="Re-download existing files."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No progress-bar."),
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=version_callback,
        help=f"Show {__package__} version and exit.",
    ),
):
    """Download Air Quality Data from the European Environment Agency (EEA)"""

    client = AirbaseClient()
    request = client.request(
        year_from=str(year), year_to=str(year), verbose=not quiet
    )
    request.download_to_directory(path, skip_existing=not overwrite)
