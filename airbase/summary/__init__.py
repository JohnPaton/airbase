from __future__ import annotations

from .db import DB

__all__ = ["DB", "COUNTRY_CODES"]


COUNTRY_CODES: set[str]


def __getattr__(name: str):
    if name == "COUNTRY_CODES":
        return set(DB.countries())
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
