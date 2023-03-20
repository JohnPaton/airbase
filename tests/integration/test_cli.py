from pathlib import Path

from click.testing import CliRunner

from airbase.cli import main

runner = CliRunner()


def test_download(tmp_path: Path):
    country, year, pollutant, id = "NO", 2021, "NO2", 8
    options = f"download --quiet --country {country} --pollutant {pollutant} --year {year} --path {tmp_path}"
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(main, options.split())
        assert result.exit_code == 0
        files = tmp_path.glob(f"{country}_{id}_*_{year}_timeseries.csv")

    assert list(files)
