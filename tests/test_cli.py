from __future__ import annotations

import pytest
from typer.testing import CliRunner

from airbase import __version__
from airbase.cli import main

runner = CliRunner()


@pytest.mark.parametrize("options", ("--version", "-V"))
def test_version(options: str):
    result = runner.invoke(main, options.split())
    assert result.exit_code == 0
    assert "airbase" in result.output
    assert str(__version__) in result.output
