from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from airbase.cli import main

runner = CliRunner()


@pytest.mark.parametrize(
    "cmd,country,city,pollutant,expected",
    (
        pytest.param(
            "download", "MT", "Valletta", "SO2",
            {"MT_1_27896_2024_timeseries.csv"},
            id="legacy",
        ),
        pytest.param(
            "historical", "MT", "Valletta", "PM2.5",
            {"MT/SPO-MT00005_06001_100.parquet"},
            id="historical",
        ),
        pytest.param(
            "verified", "MT", "Valletta", "O3",
            {"MT/SPO-MT00003_00007_100.parquet", "MT/SPO-MT00005_00007_100.parquet"},
            id="verified"),
        pytest.param(
            "unverified", "MT", "Valletta", "PM10",
            {"MT/SPO-MT00005_00005_100.parquet", "MT/SPO-MT00005_00005_101.parquet"},
            id="unverified"
        ),
    ),
)  # fmt:skip
def test_download(
    cmd: str,
    country: str,
    city: str,
    pollutant: str,
    expected: set[str],
    tmp_path: Path,
):
    options = f"{cmd} --quiet --country {country} --city {city} --pollutant {pollutant} --path {tmp_path}"
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(main, options.split())
        assert result.exit_code == 0

    found = set(tmp_path.rglob("*.*"))
    paths = set(tmp_path / file for file in expected)
    assert found >= paths > set()


@pytest.mark.parametrize(
    "cmd,country,city,pollutant,expected",
    (
        pytest.param(
            "historical", "MT", "Valletta", "PM2.5",
            "found 1 file(s), ~0 Mb in total",
            id="historical",
        ),
        pytest.param(
            "verified", "MT", "Valletta", "O3",
            "found 2 file(s), ~1 Mb in total",
            id="verified"),
        pytest.param(
            "unverified", "MT", "Valletta", "PM10",
            "found 2 file(s), ~0 Mb in total",
            id="unverified"
        ),
    ),
)  # fmt:skip
def test_summary(
    cmd: str,
    country: str,
    city: str,
    pollutant: str,
    expected: str,
    tmp_path: Path,
):
    options = f"{cmd} --quiet --country {country} --city {city} --pollutant {pollutant} --path {tmp_path} --aggregation-type hourly --summary"
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(main, options.split())
        assert result.exit_code == 0
        assert expected in result.stdout

    files = tuple(tmp_path.rglob("*.parquet"))
    assert not files
