from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from airbase import __version__
from airbase.main import main

runner = CliRunner()


@pytest.mark.parametrize("options", ("--version", "-V"))
def test_version(options: str):
    result = runner.invoke(main, options.split())
    assert result.exit_code == 0
    assert "airbase" in result.output
    assert str(__version__) in result.output


@pytest.mark.parametrize("cmd", ("historical", "verified", "unverified"))
@pytest.mark.usefixtures("mock_parquet_api")
def test_download(cmd: str, tmp_path: Path):
    tmp_path.joinpath(cmd).mkdir()
    options = f"--metadata --city Valletta --path {tmp_path} {cmd}"
    result = runner.invoke(main, f"--quiet {options}")
    assert result.exit_code == 0

    assert sum(1 for _ in tmp_path.glob(f"{cmd}/MT/*.parquet")) == 22
    assert set(tmp_path.glob("*.csv")) == {tmp_path / "metadata.csv"}


@pytest.mark.parametrize("cmd", ("historical", "verified", "unverified"))
@pytest.mark.usefixtures("mock_parquet_api")
def test_summary(cmd: str, tmp_path: Path):
    options = f"--summary --metadata --city Valletta --path {tmp_path} {cmd}"
    result = runner.invoke(main, f"--quiet {options}")
    assert result.exit_code == 0
    assert result.output.strip() == "found 22 file(s), ~11 Mb in total"

    assert not set(tmp_path.glob("*.parquet"))
    assert not set(tmp_path.glob("*.csv"))


@pytest.mark.parametrize(
    "options",
    (
        pytest.param("plugin-cmd", id="cmd"),
        pytest.param("plugin-sub first", id="sub first"),
        pytest.param("plugin-sub second", id="sub second"),
    ),
)
def test_plugin(options: str):
    result = runner.invoke(main, options)
    assert result.exit_code == 0
    assert result.output.strip() == f"{options} from plugin"
