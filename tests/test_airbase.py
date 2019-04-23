import os
import responses
import pytest

import airbase


class TestAirbaseClient:
    def test_init_connect_false(self, summary_response):
        responses.add(summary_response)
        client = airbase.AirbaseClient(connect=False)
        with pytest.raises(AttributeError):
            client.all_countries
        with pytest.raises(AttributeError):
            client.all_pollutants
        with pytest.raises(AttributeError):
            client.pollutants_per_country

        client.connect()
        assert client.all_countries is not None
        assert client.all_pollutants is not None
        assert client.pollutants_per_country is not None

    def test_init_connect(self, summary_response):
        responses.add(summary_response)
        client = airbase.AirbaseClient()
        assert client.all_countries is not None
        assert client.all_pollutants is not None
        assert client.pollutants_per_country is not None

    def test_download_metadata(self, tmpdir, metadata_response):
        fpath = str(tmpdir / "meta.csv")
        responses.add(metadata_response)
        airbase.AirbaseClient.download_metadata(fpath)
        assert os.path.exists(fpath)
        with open(fpath) as h:
            assert h.read() == metadata_response.body.decode()

    def test_request_raises_not_connected(self, summary_response):
        client = airbase.AirbaseClient(connect=False)
        with pytest.raises(AttributeError):
            client.request()

    def test_request_raises_bad_country(self, mocked_client):
        with pytest.raises(ValueError):
            mocked_client.request(country="lol123")

        with pytest.raises(ValueError):
            mocked_client.request(["NL", "lol123"])

    def test_request_raises_bad_year(self, mocked_client):
        with pytest.raises(ValueError):
            mocked_client.request(year_from=1234)

        with pytest.raises(ValueError):
            mocked_client.request(year_to=9999)

    def test_request_response_generated(self, mocker, summary_response):
        pass

    def test_request_not_pl_and_shortpl(self, mocked_client):
        with pytest.raises(ValueError):
            mocked_client.request(pl="O3", shortpl=123)
