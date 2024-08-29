import pytest

from airbase.util import string_safe_list


@pytest.mark.parametrize(
    "input,output",
    (
        pytest.param("a string", ("a string",), id="str"),
        pytest.param([1, 2, 3], (1, 2, 3), id="list[int]"),
        pytest.param(None, tuple(), id="None"),
    ),
)
def test_string_safe_list(input, output):
    assert string_safe_list(input) == output
