from __future__ import annotations

from importlib import metadata

__version__: str | None
try:
    __version__ = metadata.version("airbase")
except metadata.PackageNotFoundError:  # pragma:no cover
    # package isn't installed
    __version__ = None
