[![PyPI version](https://badge.fury.io/py/airbase.svg)](https://badge.fury.io/py/airbase)
[![Downloads](https://pepy.tech/badge/airbase)](https://pepy.tech/project/airbase)
[![CI/CD](https://github.com/JohnPaton/airbase/actions/workflows/cicd.yaml/badge.svg?branch=master)](https://github.com/JohnPaton/airbase/actions/workflows/cicd.yaml)
[![Documentation Status](https://readthedocs.org/projects/airbase/badge/?version=latest)](https://airbase.readthedocs.io/en/latest/?badge=latest)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/format.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

# 🌬 AirBase

An easy downloader for air quality data provided by the European Environment Agency (EEA).

The data is available for download at
[the portal](https://eeadmz1-downloads-webapp.azurewebsites.net/), but
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
>>> client.countries
frozenset({'LI', 'CY', 'IE', 'LV', 'BE', 'EE', ...})

>>> client.pollutants
frozenset({'Co', 'sum-PCB', 'PCB-26', 'HNO3', ...})


>>> client.search_pollutant("O3")
[{'poll': 'O3', 'id': 7}, {'poll': 'NO3', 'id': 46}, ...]
```

🗂 Request download links from the server and save the resulting Parquet files into a directory:

```pycon
>>> r = client.request("Verified", "NL", "DE", poll=["NO3", "NO3- in PM2.5", "NO3- in PM10"])
>>> r.download(dir="data", skip_existing=True)
summary : 100%|█████| 2/2 [00:00<00:00,  4.48requests/s]
URLs    : 100%|█████| 29.0/29.0 [00:00<00:00, 490URL/s]
download: 386kb [00:00, 570kb/s]  
```

📦 Download the entire dataset (not for the faint of heart):

```pycon
>>> r = client.request()
>>> r.download_to_directory("data")
summary : 100%|█████| 39/39 [00:15<00:00,  2.54requests/s]
URLs    : 100%|█████| 47.0k/47.0k [00:00<00:00, 77.7kURL/s]
download: 20.6Gb [54:36, 6.74Mb/s]    
```

🌡 Don't forget to get the metadata about the measurement stations:

```pycon
>>> client.download_metadata("data/metadata.csv")
Writing metadata to data/metadata.csv...
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
