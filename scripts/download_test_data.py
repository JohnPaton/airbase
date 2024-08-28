#!/usr/bin/env python3

import argparse
import asyncio
import json
import subprocess
from pathlib import Path

from airbase.download_api import (
    COUNTRY_CODES,
    Dataset,
    DownloadInfo,
)

BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"


async def main(root_path: Path):
    if root_path.exists() and not root_path.is_dir():
        raise NotADirectoryError(f"{root_path} should be a directory")

    path = root_path / "country.json"
    print(f"download {path}")
    cmd = (
        f"curl -s -X 'GET' '{BASE_URL}/Country'"
        " -H 'accept: text/plain'"
        f" -o {path}"
    )
    assert path.stat().st_size > 0, f"{path.name} is empty"

    path = root_path / "property.json"
    print(f"download {path}")
    cmd = (
        f"curl -s -X 'GET' '{BASE_URL}/Property'"
        " -H 'accept: text/plain'"
        f" -o {path}"
    )
    subprocess.check_call(cmd, shell=True)
    assert path.stat().st_size > 0, f"{path.name} is empty"

    path = root_path / "city.json"
    print(f"download {path}")
    cmd = (
        f"curl -s -X 'POST' '{BASE_URL}/City' "
        " -H 'accept: text/plain'"
        " -H 'Content-Type: application/json'"
        f" -d '{json.dumps(tuple(COUNTRY_CODES))}'"
        f" -o {path}"
    )
    subprocess.check_call(cmd, shell=True)
    assert path.stat().st_size > 0, f"{path.name} is empty"

    info = DownloadInfo(None, "MT", Dataset.Historical, "Valletta")
    path = root_path / f"{info.country}_{info.dataset}_{info.city}.csv"
    print(f"download {path}")
    cmd = (
        f"curl -s -X 'POST' '{BASE_URL}/ParquetFile/urls'"
        " -H 'accept: */*'"
        " -H 'Content-Type: application/json'"
        f" -d '{json.dumps(info.request_info())}'"
        f" -o {path}"
    )
    subprocess.check_call(cmd, shell=True)
    assert path.stat().st_size > 0, f"{path.name} is empty"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        usage=f"Download {BASE_URL} responses for later use on tests",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--path",
        dest="path",
        type=Path,
        default=Path("tests/resources"),
        help="test resources directory",
    )

    args = parser.parse_args()
    asyncio.run(main(args.path))
