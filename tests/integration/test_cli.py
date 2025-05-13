from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from airbase.main import main

runner = CliRunner()


@pytest.mark.parametrize(
    "cmd,pollutant,expected",
    (
        pytest.param(
            "historical", "PM2.5",
            {"SPO-MT00005_06001_100.parquet"},
            id="historical",
        ),
        pytest.param(
            "verified", "O3",
            {"SPO-MT00003_00007_100.parquet", "SPO-MT00005_00007_100.parquet"},
            id="verified"),
        pytest.param(
            "unverified", "PM10",
            {"SPO-MT00005_00005_100.parquet"},
            id="unverified"
        ),
    ),
)  # fmt:skip
def test_download(
    cmd: str,
    pollutant: str,
    expected: set[str],
    tmp_path: Path,
):
    options = f"--path {tmp_path} {cmd} -C Valletta -p {pollutant}"
    result = runner.invoke(main, f"--quiet {options}")
    assert result.exit_code == 0

    found = set(tmp_path.rglob("*.parquet"))
    paths = set(tmp_path.joinpath(cmd, "MT", file) for file in expected)
    assert found >= paths > set()
    assert not set(tmp_path.rglob("*.csv"))


@pytest.mark.parametrize(
    "cmd,city,pollutant,expected",
    (
        pytest.param(
            "historical", "Valletta", "PM2.5",
            "found 1 file(s), ~0 Mb in total",
            id="historical",
        ),
        pytest.param(
            "verified", "Valletta", "O3",
            "found 2 file(s), ~1 Mb in total",
            id="verified"),
        pytest.param(
            "unverified", "Valletta", "PM10",
            "found 2 file(s), ~0 Mb in total",
            id="unverified"
        ),
    ),
)  # fmt:skip
def test_summary(
    cmd: str,
    city: str,
    pollutant: str,
    expected: str,
    tmp_path: Path,
):
    options = f"--summary --path {tmp_path} {cmd} -C {city} -p {pollutant}"
    result = runner.invoke(main, f"--quiet {options}")
    assert result.exit_code == 0
    assert expected in result.output

    assert not set(tmp_path.rglob("*.parquet"))
    assert not set(tmp_path.rglob("*.csv"))
