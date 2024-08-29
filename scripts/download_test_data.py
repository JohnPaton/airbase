#!/usr/bin/env python3

import json
import subprocess
from pathlib import Path

from airbase.download_api import Dataset, DownloadInfo
from airbase.summary import COUNTRY_CODES

BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"


def main(data_path: Path = Path("tests/resources")):
    if data_path.exists() and not data_path.is_dir():
        raise NotADirectoryError(f"{data_path} should be a directory")

    path = data_path / "country.json"
    print(f"download {path}")
    cmd = (
        f"curl -s -X 'GET' '{BASE_URL}/Country'"
        " -H 'accept: text/plain'"
        f" -o {path}"
    )
    assert path.stat().st_size > 0, f"{path.name} is empty"

    path = data_path / "property.json"
    print(f"download {path}")
    cmd = (
        f"curl -s -X 'GET' '{BASE_URL}/Property'"
        " -H 'accept: text/plain'"
        f" -o {path}"
    )
    subprocess.check_call(cmd, shell=True)
    assert path.stat().st_size > 0, f"{path.name} is empty"

    path = data_path / "city.json"
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
    path = data_path / f"{info.country}_{info.dataset}_{info.city}.csv"
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
    main()
