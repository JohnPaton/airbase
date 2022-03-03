from airbase.summary.db import DB


def test_countries():
    countries = DB.countries()
    assert isinstance(countries, list)
    assert countries
    assert all(isinstance(country, str) for country in countries)
    for country in ["NO", "DK", "SE", "DE", "IT", "FR", "NL", "GB"]:
        assert country in countries


def test_pollutants():
    pollutants = DB.pollutants()
    assert isinstance(pollutants, dict)
    assert pollutants
    assert all(isinstance(id, int) for id in pollutants.values())
    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "PM2.5": 6001}.items():
        assert pollutants.get(poll) == id


def test_pollutants_per_country():
    output = DB.pollutants_per_country()
    assert isinstance(output, dict)
    assert output

    pollutants = output.get("AD")
    assert isinstance(pollutants, dict)
    assert all(isinstance(id, int) for id in pollutants.values())
    for poll, id in {"PM10": 5, "O3": 7, "NO2": 8, "SO2": 1}.items():
        assert pollutants.get(poll) == id
