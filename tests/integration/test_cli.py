from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from airbase.cli import main

runner = CliRunner()


def test_download(tmp_path: Path):
    commands = (
        "--quiet",
        f"historical --city Valletta --path {tmp_path} --pollutant PM2.5",
        f"  verified --city Valletta --path {tmp_path} --pollutant O3",
        f"unverified --city Valletta --path {tmp_path} --pollutant PM10",
    )

    result = runner.invoke(main, " ".join(commands))
    assert result.exit_code == 0

    tmp_path.relative_to
    found = set(tmp_path.glob("MT/*.parquet"))
    paths = {
        "historical PM2.5": {
            tmp_path / "MT/SPO-MT00005_06001_100.parquet",
        },
        "verified O3": {
            tmp_path / "MT/SPO-MT00003_00007_100.parquet",
            tmp_path / "MT/SPO-MT00005_00007_100.parquet",
        },
        "unverified PM10": {
            tmp_path / "MT/SPO-MT00005_00005_100.parquet",
            tmp_path / "MT/SPO-MT00005_00005_101.parquet",
        },
    }
    assert len(found) == sum(len(val) for val in paths.values())
    for key, val in paths.items():
        assert found > val > set(), key


def test_summary(tmp_path: Path):
    commands = (
        "--quiet --summary",
        f"historical -F hourly -C Valletta --path {tmp_path} -p PM2.5",
        f"  verified -F hourly -C Valletta --path {tmp_path} -p O3",
        f"unverified -F hourly -C Valletta --path {tmp_path} -p PM10",
    )

    result = runner.invoke(main, " ".join(commands))
    assert result.exit_code == 0

    summary = {
        "historical hourly": "found 1 file(s), ~0 Mb in total",
        "verified hourly": "found 2 file(s), ~1 Mb in total",
        "unverified hourly": "found 2 file(s), ~0 Mb in total",
    }
    for key, val in summary.items():
        assert f"{key}\n{val}\n" in result.stdout, key

    files = tuple(tmp_path.rglob("*.*"))
    assert not files
