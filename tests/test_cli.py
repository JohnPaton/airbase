from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Literal

import pytest
from typer.testing import CliRunner

from airbase import __version__
from airbase.cli import Country, Pollutant, main

if sys.version_info >= (3, 11):
    from contextlib import chdir
else:
    import os
    from contextlib import contextmanager

    @contextmanager
    def chdir(path: Path):
        old_cwd = Path.cwd()
        os.chdir(path)
        yield
        os.chdir(old_cwd)


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


@pytest.mark.parametrize("dataset", ("historical", "verified", "unverified"))
@pytest.mark.usefixtures("mock_parquet_api")
def test_download(
    tmp_path: Path, dataset: Literal["historical", "verified", "unverified"]
):
    result = runner.invoke(main, f"{dataset} --city Valletta --path {tmp_path}")
    assert result.exit_code == 0
    assert sum(1 for _ in tmp_path.glob("MT/*.parquet")) == 22


@pytest.mark.parametrize(
    "path",
    (
        pytest.param(None, id="dir"),
        pytest.param("data", id="csv"),
    ),
)
@pytest.mark.usefixtures("mock_parquet_api")
def test_metadata(tmp_path: Path, path: str | None):
    metadata = tmp_path.joinpath(path or "", "metadata.csv")
    result = runner.invoke(main, f"metadata {metadata.parent}")
    assert result.exit_code == 0
    assert metadata.is_file()


@pytest.mark.usefixtures("mock_parquet_api")
def test_download_chain(tmp_path: Path):
    commands = (
        "--quiet --no-subdir",
        "historical --city Valletta",
        "  verified --city Valletta",
        "unverified --city Valletta",
        "metadata data",
    )
    with chdir(tmp_path):
        result = runner.invoke(main, " ".join(commands))
    assert result.exit_code == 0

    counter = Counter(path.parent for path in tmp_path.rglob("*.parquet"))
    assert counter == {
        tmp_path / "data/historical": 22,
        tmp_path / "data/verified": 22,
        tmp_path / "data/unverified": 22,
    }
    metadata = tmp_path / "data/metadata.csv"
    assert metadata.is_file()


@pytest.mark.usefixtures("mock_parquet_api")
def test_summary_chain(tmp_path: Path):
    commands = (
        "--quiet --summary",
        "historical --city Valletta",
        "  verified --city Valletta",
        "unverified --city Valletta",
    )
    with chdir(tmp_path):
        result = runner.invoke(main, " ".join(commands))
        counter = Counter(path.parent for path in Path.cwd().rglob("*.parquet"))
    assert result.exit_code == 0

    summary = "found 22 file(s), ~11 Mb in total"
    assert f"historical\n{summary}\n" in result.stdout, "historical"
    assert f"verified\n{summary}\n" in result.stdout, "verified"
    assert f"unverified\n{summary}\n" in result.stdout, "unverified"

    assert counter == {}
