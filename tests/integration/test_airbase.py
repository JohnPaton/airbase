from pathlib import Path

import pytest

import airbase
from tests.resources import LEGACY_METADATA_RESPONSE


@pytest.fixture(scope="module")
def client():
    """Return initialized AirbaseClient instance"""
    return airbase.AirbaseClient()


def test_download_to_directory(client: airbase.AirbaseClient, tmp_path: Path):
    r = client.request("Historical", "AD", "BE", poll="CO")

    r.download(dir=str(tmp_path), skip_existing=True)
    assert list(tmp_path.rglob("*.parquet"))


def test_download_metadata(client: airbase.AirbaseClient, tmp_path: Path):
    path = tmp_path / "metadata.tsv"
    client.download_metadata(path)
    assert path.exists()

    # make sure metadata format hasn't changed
    headers_downloaded = path.read_text().splitlines()[0]
    headers_expected = LEGACY_METADATA_RESPONSE.splitlines()[0]

    assert headers_downloaded == headers_expected
