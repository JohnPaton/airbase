from __future__ import annotations

import pytest
from typer import Typer
from typer.testing import CliRunner

from airbase import __version__
from airbase.cli import Country, Pollutant, main

runner = CliRunner()


@pytest.mark.parametrize("country", Country)
def test_country(country: Country):
    assert country.name == country.value == str(country)


@pytest.mark.parametrize("pollutant", Pollutant)
def test_pollutant(pollutant: Pollutant):
    assert pollutant.name == pollutant.value == str(pollutant)


@pytest.mark.parametrize("options", ("--version", "-V"))
def test_version(options: str):
    result = runner.invoke(main, options.split())
    assert result.exit_code == 0
    assert "airbase" in result.output
    assert str(__version__) in result.output


def test_CO_vs_Co():
    app = Typer()

    pollutants = set()

    @app.command()
    def CO_vs_Co(poll: Pollutant):
        pollutants.add(poll)

    result = runner.invoke(app, ["CO"])
    assert result.exit_code == 0
    assert pollutants == {Pollutant.CO}
