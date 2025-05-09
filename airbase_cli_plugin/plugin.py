"""
AirBase CLI plugin using package metadata
https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata
"""

import typer


def command(ctx: typer.Context):
    """cli command from plugin"""
    typer.echo(f"{ctx.command.name} from plugin")


main = typer.Typer(no_args_is_help=True)


@main.callback()
def callback(ctx: typer.Context):
    """airbase command group"""
    typer.echo(f"{ctx.command.name} {ctx.invoked_subcommand} from plugin")


@main.command()
def first():
    """cli sub-command from plugin"""


@main.command()
def second():
    """cli sub-command from plugin"""
