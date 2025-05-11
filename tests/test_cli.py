from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Literal

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


@pytest.mark.parametrize("dataset", ("historical", "verified", "unverified"))
@pytest.mark.usefixtures("mock_parquet_api")
def test_download(
    tmp_path: Path, dataset: Literal["historical", "verified", "unverified"]
):
    result = runner.invoke(main, f"{dataset} --city Valletta --path {tmp_path}")
    assert result.exit_code == 0
    assert sum(1 for _ in tmp_path.glob("MT/*.parquet")) == 22


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


@pytest.mark.usefixtures("mock_parquet_api")
def test_download_chain(tmp_path):
    commands = (
        "--quiet --no-subdir",
        f"historical --city Valletta --path {tmp_path}/historical",
        f"  verified --city Valletta --path {tmp_path}/verified",
        f"unverified --city Valletta --path {tmp_path}/unverified",
        f"metadata {tmp_path}",
    )
    tmp_path.joinpath("historical").mkdir()
    tmp_path.joinpath("verified").mkdir()
    tmp_path.joinpath("unverified").mkdir()
    result = runner.invoke(main, " ".join(commands))
    assert result.exit_code == 0

    counter = Counter(path.parts[-2] for path in tmp_path.rglob("*.parquet"))
    assert counter == {"historical": 22, "verified": 22, "unverified": 22}
    assert tmp_path.joinpath("metadata.csv").is_file()


@pytest.mark.usefixtures("mock_parquet_api")
def test_summary_chain(tmp_path):
    commands = (
        "--quiet --summary",
        f"historical --city Valletta --path {tmp_path}",
        f"  verified --city Valletta --path {tmp_path}",
        f"unverified --city Valletta --path {tmp_path}",
    )
    result = runner.invoke(main, " ".join(commands))
    assert result.exit_code == 0

    summary = "found 22 file(s), ~11 Mb in total"
    assert f"historical\n{summary}\n" in result.stdout, "historical"
    assert f"verified\n{summary}\n" in result.stdout, "verified"
    assert f"unverified\n{summary}\n" in result.stdout, "unverified"

    files = tuple(tmp_path.rglob("*.*"))
    assert not files
