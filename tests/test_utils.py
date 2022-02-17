import pytest

from airbase import util
from tests.resources import SUMMARY


class TestStringSafeList:
    def test_string(self):
        input = "a string"
        output = [input]
        assert util.string_safe_list(input) == output

    def test_list(self):
        input = [1, 2, 3]
        output = input
        assert util.string_safe_list(input) == output

    def test_none(self):
        input = None
        output = [input]
        assert util.string_safe_list(input) == output


def test_countries_from_summary():
    output = util.countries_from_summary(SUMMARY)
    assert type(output) is list
    assert len(output) > 0
    assert type(output[0]) is str


def test_pollutants_from_summary():
    output = util.pollutants_from_summary(SUMMARY)
    assert type(output) is dict
    assert len(output) > 0
    assert "PM10" in output
    assert type(output["PM10"]) is str


def test_pollutants_per_country():
    output = util.pollutants_per_country(SUMMARY)
    assert type(output) is dict
    assert len(output) > 0
    assert "AD" in output
    assert type(output["AD"]) is list
    assert len(output["AD"]) > 0
    assert "pl" in output["AD"][0]
    assert "shortpl" in output["AD"][0]


class TestLinkListURL:
    def test_all_fields_filled(self):
        output = util.link_list_url(country="BE")
        assert "{" not in output
        assert "}" not in output

    def test_year_before_2013(self):
        with pytest.raises(ValueError):
            util.link_list_url("BE", year_from=2000)

    def test_year_after_current(self):
        with pytest.raises(ValueError):
            util.link_list_url("BE", year_to=2100)

    def test_wrong_source(self):
        with pytest.raises(ValueError):
            util.link_list_url("BE", source="invalid")
