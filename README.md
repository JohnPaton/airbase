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
$ airbase --help
Usage: airbase [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...

  Download Air Quality Data from the European Environment Agency (EEA)

  Use -n/--dry-run/--summary and -q/--quiet to request the number of files and
  estimated download size without downloading the observations, e.g
  - total download files/size for hourly verified and unverified observations
    airbase --quiet --summary \
      verified -F hourly \
      unverified -F hourly

  Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
  or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
  - download verified hourly and daily PM10 and PM2.5 observations from sites in Oslo
    into different paths in order to avoid filename collisions
    airbase --no-subdir \
      verified -p PM10 -p PM2.5 -C Oslo -F daily  --path data/daily \
      verified -p PM10 -p PM2.5 -C Oslo -F hourly --path data/hourly

Options:
  -V, --version
  -n, --dry-run, --summary  Total download files/size, nothing will be
                            downloaded.
  --subdir / --no-subdir    Download files for different counties to different
                            sub directories.  [default: subdir]
  -O, --overwrite           Re-download existing files.
  -q, --quiet               No progress-bar.
  --help                    Show this message and exit.

Commands:
  historical  Historical Airbase data delivered between 2002 and 2012...
  verified    Verified data (E1a) from 2013 to 2023 reported by countries...
  unverified  Unverified data transmitted continuously...
  metadata    Download station metadata into `PATH/metadata.csv`.
```

### Historical data delivered between 2002 and 2012

``` console
$ airbase historical --help
Usage: airbase historical [OPTIONS]

  Historical Airbase data delivered between 2002 and 2012 before Air Quality
  Directive 2008/50/EC entered into force.

  Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
  or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
  - download only from Norwegian, Danish and Finish sites
    airbase historical -c NO -c DK -c FI
  - download only SO2, PM10 and PM2.5 observations
    airbase historical -p SO2 -p PM10 -p PM2.5
  - download only PM10 and PM2.5 observations from sites in Oslo
    airbase historical -p PM10 -p PM2.5 -C Oslo

Options:
  -c, --country [AD|AL|AT|...]
  -p, --pollutant [k|V|NT|...]
  -C, --city TEXT                 Only from selected <cities> (--country
                                  option will be ignored).
  -F, --aggregation-type, --frequency [hourly|daily|other]
                                  Only hourly data, daily data or other
                                  aggregation frequency.
  --path PATH                     [default: data/historical]
  --help                          Show this message and exit.
```

### Verified data from 2013 to 2023

``` console
$ airbase verified --help
Usage: airbase verified [OPTIONS]

  Verified data (E1a) from 2013 to 2023 reported by countries by 30 September
  each year for the previous year.

  Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
  or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
  - download only from Norwegian, Danish and Finish sites
    airbase verified -c NO -c DK -c FI
  - download only SO2, PM10 and PM2.5 observations
    airbase verified -p SO2 -p PM10 -p PM2.5
  - download only PM10 and PM2.5 observations from sites in Oslo
    airbase verified -p PM10 -p PM2.5 -C Oslo

Options:
  -c, --country [AD|AL|AT|...]
  -p, --pollutant [k|V|NT|...]
  -C, --city TEXT                 Only from selected <cities> (--country
                                  option will be ignored).
  -F, --aggregation-type, --frequency [hourly|daily|other]
                                  Only hourly data, daily data or other
                                  aggregation frequency.
  --path PATH                     [default: data/verified]
  --help                          Show this message and exit.
```

### Unverified data from the beginning of 2024

``` console
$ airbase unverified --help
Usage: airbase unverified [OPTIONS]

  Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the
  beginning of 2024.

  Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
  or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
  - download only from Norwegian, Danish and Finish sites
    airbase unverified -c NO -c DK -c FI
  - download only SO2, PM10 and PM2.5 observations
    airbase unverified -p SO2 -p PM10 -p PM2.5
  - download only PM10 and PM2.5 observations from sites in Oslo
    airbase unverified -p PM10 -p PM2.5 -C Oslo

Options:
  -c, --country [AD|AL|AT|...]
  -p, --pollutant [k|V|NT|...]
  -C, --city TEXT                 Only from selected <cities> (--country
                                  option will be ignored).
  -F, --aggregation-type, --frequency [hourly|daily|other]
                                  Only hourly data, daily data or other
                                  aggregation frequency.
  --path PATH                     [default: data/unverified]
  --help                          Show this message and exit.
```

### Station metadata

``` console
$ airbase metadata --help
Usage: airbase metadata [OPTIONS]

  Download station metadata into `PATH/metadata.csv`.

  Use chan notation to donwload metadata and observations, e.g.
  - donwload station metadata and hourly PM10 and PM2.5 observations
    from sites in Oslo into into different paths
      airbase --quiet --no-subdir \
        historical --path data/historical -F hourly -p PM10 -p PM2.5 -C Oslo \
        verified   --path data/verified   -F hourly -p PM10 -p PM2.5 -C Oslo \
        unverified --path data/unverified -F hourly -p PM10 -p PM2.5 -C Oslo \
        metadata   --path data/

Options:
  --path PATH  [default: data]
  --help       Show this message and exit.
```

## 🛣 Roadmap

* ~~Parallel CSV downloads~~ Contributed by @avaldebe
* ~~CLI to avoid using Python all together~~ Contributed by @avaldebe
* Data wrangling module for AirBase output data
