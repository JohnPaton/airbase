import sys

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

try:
    __version__ = metadata.version("airbase")
except metadata.PackageNotFoundError:
    # package isn't installed
    __version__ = None
