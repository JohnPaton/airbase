from pathlib import Path

import pytest
from typer.testing import CliRunner

from airbase.cli import main

runner = CliRunner()


@pytest.mark.parametrize(
    "cmd,country,city,num",
    (
        pytest.param("historical", "MT", "Valletta", 22, id="historical"),
        pytest.param("verified", "MT", "Valletta", 48, id="verified"),
        pytest.param("unverified", "MT", "Valletta", 48, id="unverified"),
    ),
)
def test_download(cmd: str, country: str, city: str, num: int, tmp_path: Path):
    options = (
        f"{cmd} --quiet --country {country} --city {city} --path {tmp_path}"
    )
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(main, options.split())
        assert result.exit_code == 0

    files = sorted(tmp_path.glob(f"{country}/*.parquet"))
    assert len(files) >= num
