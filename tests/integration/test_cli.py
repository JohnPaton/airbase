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
            "historical", "MT", "Valletta", "PM2.5",
            {"SPO-MT00005_06001_100.parquet"},
            id="historical",
        ),
        pytest.param(
            "verified", "MT", "Valletta", "O3",
            {"SPO-MT00003_00007_100.parquet", "SPO-MT00005_00007_100.parquet"},
            id="verified"),
        pytest.param(
            "unverified", "MT", "Valletta", "PM10",
            {"SPO-MT00003_00005_100.parquet", "SPO-MT00003_00005_101.parquet",
             "SPO-MT00005_00005_100.parquet", "SPO-MT00005_00005_101.parquet"},
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

    files = set(path.name for path in tmp_path.glob(f"{country}/*.parquet"))
    assert files >= expected > set()
