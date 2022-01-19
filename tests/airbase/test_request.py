from pathlib import Path

import pytest

import airbase
from tests import resources


@pytest.fixture()
def all_responses(
    summary_response, csv_links_response, csv_response, metadata_response
):
    """mock responses from relevant urls"""
    return


@pytest.fixture
def r(all_responses) -> airbase.AirbaseRequest:
    """initialied request with mocked responses"""
    return airbase.AirbaseRequest()


def test_preload_csv_links(all_responses):
    r = airbase.AirbaseRequest(preload_csv_links=False)
    assert r._csv_links == []

    r = airbase.AirbaseRequest(preload_csv_links=True)
    assert len(r._csv_links) > 0


def test_verbose_produces_output(all_responses, capsys, tmp_path: Path):
    r = airbase.AirbaseRequest(verbose=False, preload_csv_links=True)
    r.download_to_directory(str(tmp_path))

    output = capsys.readouterr()
    assert len(output.out) == 0
    assert len(output.err) == 0

    r = airbase.AirbaseRequest(verbose=True, preload_csv_links=True)
    r.download_to_directory(str(tmp_path))

    output = capsys.readouterr()
    assert len(output.out) == 0
    assert len(output.err) > 0


def test_directory_must_exist(r: airbase.AirbaseRequest):
    with pytest.raises(NotADirectoryError):
        r.download_to_directory("does/not/exist")


def test_download_to_directory_files_written(
    r: airbase.AirbaseRequest, tmp_path: Path
):
    r.download_to_directory(str(tmp_path))
    assert list(tmp_path.glob("*.csv"))


def test_download_file_directory_must_exist(r: airbase.AirbaseRequest):
    with pytest.raises(NotADirectoryError):
        r.download_to_file("does/not/exist.csv")


def test_download_file_curdir(
    r: airbase.AirbaseRequest, tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(str(tmp_path))
    r.download_to_file("test.csv")
    assert Path("test.csv").exists()


def test_download_file(r: airbase.AirbaseRequest, tmp_path: Path):
    path = tmp_path / "test.csv"
    r.download_to_file(str(path))
    assert path.exists()

    header_expected = resources.CSV_RESPONSE.splitlines()[0]
    lines = path.read_text().splitlines()

    # make sure header written
    header = lines[0]
    assert header == header_expected

    # make sure header only there once
    is_header = [line == header_expected for line in lines]
    assert sum(is_header) == 1


def test_download_metadata(r: airbase.AirbaseRequest, tmp_path: Path):
    with pytest.raises(NotADirectoryError):
        r.download_metadata("does/not/exist.tsv")

    path = tmp_path / "meta.tsv"
    r.download_metadata(str(path))
    assert path.exists()


def test_download_metadata_curdir(
    r: airbase.AirbaseRequest, tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(str(tmp_path))

    path = Path("meta.tsv")
    r.download_metadata(path.name)
    assert path.exists()
