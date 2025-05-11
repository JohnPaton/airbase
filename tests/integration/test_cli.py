from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

from typer.testing import CliRunner

from airbase.cli import main

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


def test_download(tmp_path: Path):
    commands = (
        "--quiet",
        "historical --city Valletta --pollutant PM2.5",
        "  verified --city Valletta --pollutant O3",
        "unverified --city Valletta --pollutant PM10",
    )
    with chdir(tmp_path):
        result = runner.invoke(main, " ".join(commands))
    assert result.exit_code == 0

    found: dict[str, set[str]] = defaultdict(set)
    for path in tmp_path.rglob("*.parquet"):
        found[path.parts[-3]].add(path.stem)

    assert found == {
        "historical": {"SPO-MT00005_06001_100"},
        "verified": {"SPO-MT00003_00007_100", "SPO-MT00005_00007_100"},
        "unverified": {"SPO-MT00005_00005_100", "SPO-MT00005_00005_101"},
    }


def test_summary(tmp_path: Path):
    commands = (
        "--quiet --summary",
        "historical -F hourly -C Valletta -p PM2.5",
        "  verified -F hourly -C Valletta -p O3",
        "unverified -F hourly -C Valletta -p PM10",
    )
    with chdir(tmp_path):
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
