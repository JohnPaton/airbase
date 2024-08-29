"""Utility functions for processing the raw Portal responses, url templating, etc."""

from __future__ import annotations

from typing import Iterable, overload


@overload
def string_safe_list(obj: None) -> tuple[()]:  # pragma: no cover
    ...


@overload
def string_safe_list(
    obj: str | Iterable[str],
) -> tuple[str, ...]:  # pragma: no cover
    ...


@overload
def string_safe_list(
    obj: int | Iterable[int],
) -> tuple[int, ...]:  # pragma: no cover
    ...


def string_safe_list(obj):
    """
    Turn an (iterable) object into a list. If it is a string or not
    iterable, put the whole object into a list of length 1.

    :param obj:
    :return list:
    """
    if obj is None:
        return tuple()
    if isinstance(obj, (str, int)):
        return (obj,)
    return tuple(obj)
