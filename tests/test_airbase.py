from __future__ import annotations

from pathlib import Path

import pytest

import airbase


@pytest.fixture
def client(mock_parquet_api, mock_csv_api) -> airbase.AirbaseClient:
    """initialized client with mocked responses"""
    return airbase.AirbaseClient()


class TestAirbaseClient:
    def test_init(self, client: airbase.AirbaseClient):
        assert isinstance(client.countries, frozenset)
        assert isinstance(client.pollutants, frozenset)

    def test_download_metadata(
        self, tmp_path: Path, client: airbase.AirbaseClient
    ):
        path = tmp_path / "meta.csv"
        client.download_metadata(path)
        assert path.exists()

        metadata = path.read_text().splitlines()
        assert len(metadata) == 276

        header = metadata[0].split(",")
        assert len(header) == 70

    def test_request_raises_bad_country(self, client: airbase.AirbaseClient):
        with pytest.raises(ValueError):
            client.request("Historical", "lol123")

        with pytest.raises(ValueError):
            client.request("Historical", "NL", "lol123")

    def test_request_pl(self, client: airbase.AirbaseClient):
        r = client.request("Historical", poll="NO")
        assert r.pollutants == {"NO"}

        r = client.request("Historical", poll=["NO", "NO3"])
        assert r.pollutants == {"NO", "NO3"}

        with pytest.raises(ValueError):
            r = client.request("Historical", poll=["NO", "NO3", "Not a pl"])

    def test_request_response_generated(self, client: airbase.AirbaseClient):
        r = client.request("Historical")
        assert isinstance(r, airbase.AirbaseRequest)

    def test_search_pl_exact(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("NO3")
        assert result[0]["poll"] == "NO3"

    def test_search_pl_shortest_first(self, client: airbase.AirbaseClient):
        result = client.search_pollutant("N")
        names: list[str] = [r["poll"] for r in result]
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
        assert result[0]["poll"] == "NO3"


@pytest.mark.usefixtures("mock_parquet_api", "mock_csv_api")
class TestAirbaseRequest:
    def test_verbose_produces_output(self, capsys, tmp_path: Path):
        r = airbase.AirbaseRequest(
            airbase.Dataset.Historical, "MT", verbose=False
        )
        r.download(tmp_path)

        output = capsys.readouterr()
        assert len(output.out) == 0
        assert len(output.err) == 0

        r = airbase.AirbaseRequest(
            airbase.Dataset.Historical, "MT", verbose=True
        )
        r.download(tmp_path)

        output = capsys.readouterr()
        assert len(output.out) == 0
        assert len(output.err) > 0

    def test_directory_must_exist(self):
        r = airbase.AirbaseRequest(airbase.Dataset.Historical)
        with pytest.raises(NotADirectoryError):
            r.download("does/not/exist")

    def test_download_to_directory_files_written(self, tmp_path: Path):
        r = airbase.AirbaseRequest(airbase.Dataset.Historical, "MT")
        r.download(tmp_path)
        assert list(tmp_path.rglob("*.parquet"))

    def test_download_metadata(self, tmp_path: Path):
        r = airbase.AirbaseRequest(airbase.Dataset.Historical)

        with pytest.raises(NotADirectoryError):
            r.download_metadata("does/not/exist.tsv")

        path = tmp_path / "meta.tsv"
        r.download_metadata(path)
        assert path.exists()

    def test_download_metadata_curdir(self, tmp_path: Path, monkeypatch):
        path = tmp_path / "meta.tsv"
        monkeypatch.chdir(path.parent)

        r = airbase.AirbaseRequest(airbase.Dataset.Historical)
        r.download_metadata(path.name)
        assert path.exists()
