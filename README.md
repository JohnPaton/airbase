[![PyPI version](https://badge.fury.io/py/airbase.svg)](https://badge.fury.io/py/airbase)
[![Downloads](https://pepy.tech/badge/airbase)](https://pepy.tech/project/airbase)
[![CI/CD](https://github.com/JohnPaton/airbase/actions/workflows/cicd.yaml/badge.svg?branch=master)](https://github.com/JohnPaton/airbase/actions/workflows/cicd.yaml)
[![Documentation Status](https://readthedocs.org/projects/airbase/badge/?version=latest)](https://airbase.readthedocs.io/en/latest/?badge=latest)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/format.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

# 🌬 AirBase

An easy downloader for the AirBase air quality data.

AirBase is an air quality database provided by the European Environment Agency
(EEA). The data is available for download at
[the portal](http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm), but
the interface makes it a bit time consuming to do bulk downloads. Hence, an easy
Python-based interface.

Read the full documentation at https://airbase.readthedocs.io.

## 🔌 Installation

To install `airbase`, simply run

```bash
$ pip install airbase
```

## 🚀 Getting Started

🗺 Get info about available countries and pollutants:

```pycon
>>> import airbase
>>> client = airbase.AirbaseClient()
>>> client.all_countries
['GR', 'ES', 'IS', 'CY', 'NL', 'AT', 'LV', 'BE', 'CH', 'EE', 'FR', 'DE', ...

>>> client.all_pollutants
{'k': 412, 'CO': 10, 'NO': 38, 'O3': 7, 'As': 2018, 'Cd': 2014, ...

>>> client.pollutants_per_country
{'AD': [{'pl': 'CO', 'shortpl': 10}, {'pl': 'NO', 'shortpl': 38}, ...

>>> client.search_pollutant("O3")
[{'pl': 'O3', 'shortpl': 7}, {'pl': 'NO3', 'shortpl': 46}, ...
```

🗂 Request download links from the server and save the resulting CSVs into a directory:

```pycon
>>> r = client.request(country=["NL", "DE"], pl="NO3", year_from=2015)
>>> r.download_to_directory(dir="data", skip_existing=True)
Generating CSV download links...
100%|██████████| 2/2 [00:03<00:00,  2.03s/it]
Generated 12 CSV links ready for downloading
Downloading CSVs to data...
100%|██████████| 12/12 [00:01<00:00,  8.44it/s]
```

💾 Or concatenate them into one big file:

```pycon
>>> r = client.request(country="FR", pl=["O3", "PM10"], year_to=2014)
>>> r.download_to_file("data/raw.csv")
Generating CSV download links...
100%|██████████| 2/2 [00:12<00:00,  7.40s/it]
Generated 2,029 CSV links ready for downloading
Writing data to data/raw.csv...
100%|██████████| 2029/2029 [31:23<00:00,  1.04it/s]
```

📦 Download the entire dataset (not for the faint of heart):

```pycon
>>> r = client.request()
>>> r.download_to_directory("data")
Generating CSV download links...
100%|██████████| 40/40 [03:38<00:00,  2.29s/it]
Generated 146,993 CSV links ready for downloading
Downloading CSVs to data...
  0%|          | 299/146993 [01:50<17:15:06,  2.36it/s]
```

🌡 Don't forget to get the metadata about the measurement stations:

```pycon
>>> client.download_metadata("data/metadata.tsv")
Writing metadata to data/metadata.tsv...
```

## 🚆 Command line interface

``` console
$ airbase download --help
Usage: airbase download [OPTIONS]
  Download all pollutants for all countries

  The -c/--country and -p/--pollutant allow to specify which data to download, e.g.
  - download only Norwegian, Danish and Finish sites
    airbase download -c NO -c DK -c FI
  - download only SO2, PM10 and PM2.5 observations
    airbase download -p SO2 -p PM10 -p PM2.5

Options:
  -c, --country [AD|AL|AT|...]
  -p, --pollutant [k|CO|NO|...]
  --path PATH                     [default: data]
  --year INTEGER                  [default: 2022]
  -O, --overwrite                 Re-download existing files.
  -q, --quiet                     No progress-bar.
  --help                          Show this message and exit.
```

## 🛣 Roadmap

* ~~Parallel CSV downloads~~ Contributed by @avaldebe
* ~~CLI to avoid using Python all together~~ Contributed by @avaldebe
* Data wrangling module for AirBase output data
