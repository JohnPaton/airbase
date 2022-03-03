from __future__ import annotations

from pathlib import Path

import pytest

import airbase
from tests import resources


@pytest.fixture
def client(metadata_response) -> airbase.AirbaseClient:
    """initialied client with mocked responses"""
    return airbase.AirbaseClient()


@pytest.fixture()
def all_responses(csv_links_response, csv_response, metadata_response):
    """mock responses from relevant urls"""
    return


class TestAirbaseClient:
    def test_init(self):
        client = airbase.AirbaseClient()
        assert isinstance(client.all_countries, list)
        assert isinstance(client.all_pollutants, dict)
        assert isinstance(client.pollutants_per_country, dict)

    def test_download_metadata(
        self, tmp_path: Path, capsys, client: airbase.AirbaseClient
    ):
        path = tmp_path / "meta.csv"
        client.download_metadata(path)
        assert path.exists()
        assert path.read_text() == resources.METADATA_RESPONSE

    def test_request_raises_bad_country(self, client: airbase.AirbaseClient):
        with pytest.raises(ValueError):
            client.request(country="lol123")

        with pytest.raises(ValueError):
            client.request(["NL", "lol123"])

    def test_request_raises_bad_year(self, client: airbase.AirbaseClient):
        with pytest.raises(ValueError):
            client.request(year_from="1234")
            client.request(year_to="1234")

        with pytest.raises(ValueError):
            client.request(year_from="9999")
            client.request(year_to="9999")

    def test_request_pl(self, client: airbase.AirbaseClient):
        r = client.request(pl="NO")
        assert r.shortpl is not None
        assert len(r.shortpl) == 1

        r = client.request(pl=["NO", "NO3"])
        assert r.shortpl is not None
        assert len(r.shortpl) == 2

        with pytest.raises(ValueError):
            r = client.request(pl=["NO", "NO3", "Not a pl"])

    def test_request_response_generated(self, client: airbase.AirbaseClient):
        r = client.request()
        assert isinstance(r, airbase.AirbaseRequest)

    def test_request_not_pl_and_shortpl(self, client: airbase.AirbaseClient):
        with pytest.raises(ValueError):
            client.request(pl="O3", shortpl="123")

    def test_search_pl_exact(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("NO3")
        assert result[0]["pl"] == "NO3"

    def test_search_pl_shortest_first(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("N")
        names: list[str] = [r["pl"] for r in result]
        assert len(names[0]) <= len(names[1])
        assert len(names[0]) <= len(names[-1])

    def test_search_pl_limit(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("N", limit=1)
        assert len(result) == 1

    def test_search_pl_no_result(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("Definitely not a pollutant")
        assert result == []

    def test_search_pl_case_insensitive(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("no3")
        assert result[0]["pl"] == "NO3"


@pytest.mark.usefixtures("all_responses")
class TestAirbaseRequest:
    def test_preload_csv_links(self):
        r = airbase.AirbaseRequest(preload_csv_links=False)
        assert r._csv_links == []

        r = airbase.AirbaseRequest(preload_csv_links=True)
        assert len(r._csv_links) > 0

    def test_verbose_produces_output(self, capsys, tmp_path: Path):
        r = airbase.AirbaseRequest(verbose=False, preload_csv_links=True)
        r.download_to_directory(tmp_path)

        output = capsys.readouterr()
        assert len(output.out) == 0
        assert len(output.err) == 0

        r = airbase.AirbaseRequest(verbose=True, preload_csv_links=True)
        r.download_to_directory(tmp_path)

        output = capsys.readouterr()
        assert len(output.out) == 0
        assert len(output.err) > 0

    def test_directory_must_exist(self):
        r = airbase.AirbaseRequest()
        with pytest.raises(NotADirectoryError):
            r.download_to_directory("does/not/exist")

    def test_download_to_directory_files_written(self, tmp_path: Path):
        r = airbase.AirbaseRequest()
        r.download_to_directory(tmp_path)
        assert list(tmp_path.glob("*.csv"))

    def test_download_file_directory_must_exist(self):
        r = airbase.AirbaseRequest()
        with pytest.raises(NotADirectoryError):
            r.download_to_file("does/not/exist.csv")

    def test_download_file_curdir(self, tmp_path: Path, monkeypatch):
        path = tmp_path / Path("test.csv")
        monkeypatch.chdir(path.parent)

        r = airbase.AirbaseRequest()
        r.download_to_file(path.name)
        assert path.exists()

    def test_download_file(self, tmp_path: Path):
        path = tmp_path / "test.csv"

        r = airbase.AirbaseRequest()
        r.download_to_file(path)
        assert path.exists()

        # make sure header written
        lines = path.read_text().splitlines()

        header = lines[0]
        header_expected = resources.CSV_RESPONSE.splitlines()[0]
        assert header == header_expected

        # make sure header only there once
        is_header = (l.strip() == header_expected for l in lines)
        assert sum(is_header) == 1

    def test_download_metadata(self, tmp_path: Path):
        r = airbase.AirbaseRequest()

        with pytest.raises(NotADirectoryError):
            r.download_metadata("does/not/exist.tsv")

        path = tmp_path / "meta.tsv"
        r.download_metadata(path)
        assert path.exists()

    def test_download_metadata_curdir(self, tmp_path: Path, monkeypatch):
        path = tmp_path / "meta.tsv"
        monkeypatch.chdir(path.parent)

        r = airbase.AirbaseRequest()
        r.download_metadata(path.name)
        assert path.exists
