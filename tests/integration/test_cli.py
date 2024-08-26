from pathlib import Path

import pytest
from typer.testing import CliRunner

from airbase.cli import main

runner = CliRunner()


@pytest.mark.parametrize(
    "country,year,pollutant,id",
    (
        pytest.param("NO", 2021, "NO2", 8, id="NO2"),
        pytest.param("NO", 2021, "CO", 10, id="CO"),
    ),
)
def test_download(
    country: str, year: int, pollutant: str, id: int, tmp_path: Path
):
    options = f"download --quiet --country {country} --pollutant {pollutant} --year {year} --path {tmp_path}"
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(main, options.split())
        assert result.exit_code == 0
        files = tmp_path.glob(f"{country}_{id}_*_{year}_timeseries.csv")

    assert list(files)


@pytest.mark.parametrize(
    "country,pollutant,city",
    (
        pytest.param("NO", "NO2", "Bergen", id="NO2"),
        pytest.param("NO", "CO", "Bergen", id="CO"),
    ),
)
def test_historical(country: str, pollutant: str, city: str, tmp_path: Path):
    options = f"historical --quiet --country {country} --pollutant {pollutant} --city {city} --path {tmp_path}"
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(main, options.split())
        assert result.exit_code == 0

    files = tmp_path.glob(f"{country}/*.parquet")
    assert list(files)
