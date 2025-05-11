from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from airbase import __version__
from airbase.cli import Country, Pollutant, main

runner = CliRunner()


@pytest.mark.parametrize("country", Country)
def test_country(country: Country):
    assert country.name == country.value == str(country)


@pytest.mark.parametrize("pollutant", Pollutant)
def test_pollutant(pollutant: Pollutant):
    assert pollutant.name == pollutant.value == str(pollutant)


@pytest.mark.parametrize("options", ("--version", "-V"))
def test_version(options: str):
    result = runner.invoke(main, options)
    assert result.exit_code == 0
    assert "airbase" in result.output
    assert str(__version__) in result.output


@pytest.mark.parametrize(
    "metadata",
    (
        pytest.param(None, id="dir"),
        pytest.param("meta.csv", id="csv"),
    ),
)
@pytest.mark.usefixtures("mock_parquet_api")
def test_metadata(tmp_path: Path, metadata: str | None):
    result = runner.invoke(main, f"metadata {tmp_path}/{metadata or ''}")
    assert result.exit_code == 0
    assert tmp_path.joinpath(metadata or "metadata.csv").is_file()
