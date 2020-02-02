import os
import glob

import pytest

import airbase
from . import resources


@pytest.mark.usefixtures("all_responses")
class TestAirbaseClient:
    def test_init_connect_false(self, summary_response):
        client = airbase.AirbaseClient(connect=False)
        with pytest.raises(AttributeError):
            client.all_countries
        with pytest.raises(AttributeError):
            client.all_pollutants
        with pytest.raises(AttributeError):
            client.pollutants_per_country
        with pytest.raises(AttributeError):
            client.request()

        client.connect()
        assert client.all_countries is not None
        assert client.all_pollutants is not None
        assert client.pollutants_per_country is not None
        assert client.request() is not None

    def test_init_connect(self, summary_response):
        client = airbase.AirbaseClient(connect=True)
        assert client.all_countries is not None
        assert client.all_pollutants is not None
        assert client.pollutants_per_country is not None

    def test_download_metadata(self, tmpdir, metadata_response, capsys):
        fpath = str(tmpdir / "meta.csv")
        client = airbase.AirbaseClient()
        client.download_metadata(fpath)
        assert os.path.exists(fpath)
        with open(fpath) as h:
            assert h.read() == metadata_response.body.decode()

    def test_request_raises_bad_country(self):
        client = airbase.AirbaseClient()
        with pytest.raises(ValueError):
            client.request(country="lol123")

        with pytest.raises(ValueError):
            client.request(["NL", "lol123"])

    def test_request_raises_bad_year(self):
        client = airbase.AirbaseClient()
        with pytest.raises(ValueError):
            client.request(year_from="1234")
            client.request(year_to="1234")

        with pytest.raises(ValueError):
            client.request(year_from="9999")
            client.request(year_to="9999")

    def test_request_pl(self):
        client = airbase.AirbaseClient()
        r = client.request(pl="NO")
        assert len(r.shortpl) == 1

        r = client.request(pl=["NO", "NO3"])
        assert len(r.shortpl) == 2

        with pytest.raises(ValueError):
            r = client.request(pl=["NO", "NO3", "Not a pl"])

    def test_request_response_generated(self,):
        client = airbase.AirbaseClient()
        r = client.request()
        assert isinstance(r, airbase.AirbaseRequest)

    def test_request_not_pl_and_shortpl(self):
        client = airbase.AirbaseClient()
        with pytest.raises(ValueError):
            client.request(pl="O3", shortpl="123")

    def test_search_pl_exact(self):
        client = airbase.AirbaseClient()
        result = client.search_pollutant("NO3")
        assert result[0]["pl"] == "NO3"

    def test_search_pl_shortest_first(self):
        client = airbase.AirbaseClient()
        result = client.search_pollutant("N")
        names = [r["pl"] for r in result]
        assert len(names[0]) <= len(names[1])
        assert len(names[0]) <= len(names[-1])

    def test_search_pl_limit(self):
        client = airbase.AirbaseClient()
        result = client.search_pollutant("N", limit=1)
        assert len(result) == 1

    def test_search_pl_no_result(self):
        client = airbase.AirbaseClient()
        result = client.search_pollutant("Definitely not a pollutant")
        assert result == []

    def test_saerch_pl_case_insensitive(self):
        client = airbase.AirbaseClient()
        result = client.search_pollutant("no3")
        assert result[0]["pl"] == "NO3"


@pytest.mark.usefixtures("all_responses")
class TestAirbaseRequest:
    def test_preload_csv_links(self):
        r = airbase.AirbaseRequest(preload_csv_links=False)
        assert r._csv_links == []

        r = airbase.AirbaseRequest(preload_csv_links=True)
        assert len(r._csv_links) > 0

    def test_verbose_produces_output(self, capsys, tmpdir):
        r = airbase.AirbaseRequest(verbose=False, preload_csv_links=True)
        r.download_to_directory(str(tmpdir))

        output = capsys.readouterr()
        assert len(output.out) == 0
        assert len(output.err) == 0

        r = airbase.AirbaseRequest(verbose=True, preload_csv_links=True)
        r.download_to_directory(str(tmpdir))

        output = capsys.readouterr()
        assert len(output.out) == 0
        assert len(output.err) > 0

    def test_directory_must_exist(self):
        r = airbase.AirbaseRequest()
        with pytest.raises(NotADirectoryError):
            r.download_to_directory("does/not/exist")

    def test_download_to_directory_files_written(self, tmpdir):
        r = airbase.AirbaseRequest()
        r.download_to_directory(str(tmpdir))
        assert len(glob.glob(str(tmpdir / "*.csv"))) > 0

    def test_download_file_directory_must_exist(self):
        r = airbase.AirbaseRequest()
        with pytest.raises(NotADirectoryError):
            r.download_to_file("does/not/exist.csv")

    def test_download_file_curdir(self, tmpdir, monkeypatch):
        monkeypatch.chdir(str(tmpdir))
        r = airbase.AirbaseRequest()
        r.download_to_file("test.csv")
        assert os.path.exists("test.csv")

    def test_download_file(self, tmpdir):
        r = airbase.AirbaseRequest()
        fpath = str(tmpdir / "test.csv")
        r.download_to_file(fpath)
        assert os.path.exists(fpath)

        # make sure header written
        with open(fpath) as h:
            lines = h.readlines()

        header = lines[0].strip()
        header_expected = resources.CSV_RESPONSE.split("\n")[0]
        assert header == header_expected

        # make sure header only there once
        is_header = [l.strip() == header_expected for l in lines]
        assert sum(is_header) == 1

    def test_download_metadata(self, tmpdir):
        r = airbase.AirbaseRequest()

        with pytest.raises(NotADirectoryError):
            r.download_metadata("does/not/exist.tsv")

        r.download_metadata(str(tmpdir / "meta.tsv"))
        assert os.path.exists(str(tmpdir / "meta.tsv"))

    def test_download_metadata_curdir(self, tmpdir, monkeypatch):
        r = airbase.AirbaseRequest()
        monkeypatch.chdir(str(tmpdir))
        r.download_metadata("meta.tsv")
        assert os.path.exists("meta.tsv")
