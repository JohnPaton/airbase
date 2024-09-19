from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pytest

import airbase
from tests import resources


@pytest.fixture(scope="module")
def client():
    """Return initialized AirbaseClient instance"""
    return airbase.AirbaseClient()


@pytest.fixture
def metadata_csv() -> str:
    payload = resources.ZIP_CSV_METADATA_RESPONSE
    with ZipFile(BytesIO(payload)) as zip:
        return zip.read("DataExtract.csv").decode()


def test_download_to_directory(client: airbase.AirbaseClient, tmp_path: Path):
    r = client.request("Historical", "AD", "BE", poll="CO")

    r.download(dir=str(tmp_path), skip_existing=True)
    assert list(tmp_path.rglob("*.parquet"))


def test_download_metadata(
    client: airbase.AirbaseClient, tmp_path: Path, metadata_csv: str
):
    path = tmp_path / "metadata.csv"
    client.download_metadata(path)
    assert path.exists()

    # make sure metadata format hasn't changed
    headers_downloaded = path.read_text().splitlines()[0]
    headers_expected = metadata_csv.splitlines()[0]

    assert headers_downloaded == headers_expected
