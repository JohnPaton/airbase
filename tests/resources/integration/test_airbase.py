import airbase
import os


def test_quickstart(tmpdir):
    client = airbase.AirbaseClient()
    assert client.all_countries is not None
    assert client.all_pollutants is not None
    assert client.pollutants_per_country is not None
    assert client.search_pollutant("O3") is not None

    r = client.request(country=["NL", "DE"], pl="NO3", year_from="2018", year_to="2018")
    r.download_to_directory(dir=tmpdir, skip_existing=True)
    assert os.listdir(tmpdir)

    r = client.request(
        country="FR", pl=["O3", "PM10"], year_from="2014", year_to="2014"
    )
    r.download_to_file(tmpdir / "raw.csv")
    assert os.path.exists(tmpdir / "raw.csv")

    client.download_metadata(tmpdir / "metadata.csv")
    assert os.path.exists(tmpdir / "metadata.csv")
