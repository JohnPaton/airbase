#!/usr/bin/env python3

import json
import subprocess
from pathlib import Path

from airbase.download_api import Dataset, DownloadInfo

BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"


def main(data_path: Path = Path("tests/resources")):
    if data_path.exists() and not data_path.is_dir():
        raise NotADirectoryError(f"{data_path} should be a directory")

    info = DownloadInfo("MT", Dataset.Historical, city="Valletta")
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
