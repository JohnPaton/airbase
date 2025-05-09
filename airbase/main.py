import sys
from importlib import metadata

import typer

from .cli import main

if sys.version_info >= (3, 10):
    ENTRY_POINTS = {
        ep.name: ep.load() for ep in metadata.entry_points(group="airbase.cli")
    }
else:
    ENTRY_POINTS = {
        ep.name: ep.load()
        for ep in metadata.entry_points().get("airbase.cli", [])
    }

for name, plugin in ENTRY_POINTS.items():
    if isinstance(plugin, typer.Typer):
        main.add_typer(plugin, name=name)
    else:
        main.command(name=name)(plugin)
