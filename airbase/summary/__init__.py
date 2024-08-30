from __future__ import annotations

from itertools import chain

from .db import DB

__all__ = ["DB", "COUNTRY_CODES", "POLLUTANT_NAMES", "POLLUTANT_IDS"]


COUNTRY_CODES: set[str]
POLLUTANT_NAMES: set[str]
POLLUTANT_IDS: set[int]


def __getattr__(name: str):
    if name == "COUNTRY_CODES":
        return set(DB.countries())
    if name == "POLLUTANT_NAMES":
        return set(DB.pollutants())
    if name == "POLLUTANT_IDS":
        return set(chain.from_iterable(DB.pollutants().values()))
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
