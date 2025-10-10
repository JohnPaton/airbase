from importlib import metadata

import typer

from .cli import main

ENTRY_POINTS = {
    ep.name: ep.load() for ep in metadata.entry_points(group="airbase.cli")
}

TEST_ENTRY_POINTS = {"plugin-sub", "plugin-cmd"}

for name, plugin in ENTRY_POINTS.items():
    if isinstance(plugin, typer.Typer):
        main.add_typer(plugin, name=name, hidden=name in TEST_ENTRY_POINTS)
    else:
        main.command(name=name, hidden=name in TEST_ENTRY_POINTS)(plugin)
