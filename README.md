# AirBase

An easy downloader for the AirBase air quality data.

AirBase is an air quality database provided by the European Environment Agency
(EEA). The data is available for download at
[the portal](http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm), but
the interface makes it a bit time consuming to do bulk downloads. Hence, an easy
Python-based interface.

## Installation

To install `airbase`, just clone this repository and install the Python package.

```bash
$ git clone https://github.com/johnpaton/airbase
$ cd airbase
$ pip install .
```

## Getting Started

Get info about available countries and pollutants:

```python
>>> import airbase
>>> client = airbase.AirbaseClient()
>>> client.all_countries
['GR', 'ES', 'IS', 'CY', 'NL', 'AT', 'LV', 'BE', 'CH', 'EE', 'FR', 'DE', ...

>>> client.all_pollutants
{'CO': '10', 'NO': '38', 'NO2': '8', 'NOX as NO2': '9', 'O3': '7', ...

>>> client.pollutants_per_country
{'AD': [{'pl': 'CO', 'shortpl': '10'}, {'pl': 'NO', 'shortpl': '38'}, ...

>>> client.search_pollutant("O3")
[{'pl': 'O3', 'shortpl': '7'}, {'pl': 'NO3', 'shortpl': '46'}, ...
```

Request download links from the server and save the resulting CSVs into a directory:

```python
>>> r = client.request(country=["NL", "DE"], pl="NO3", year_from=2015)
>>> r.download_to_directory(dir="data", skip_existing=True)
```
```
Generating CSV download links...
100%|██████████| 2/2 [00:03<00:00,  2.03s/it]
Generated 12 CSV links ready for downloading
Downloading CSVs to data...
100%|██████████| 12/12 [00:01<00:00,  8.44it/s]
```

Or concatenate them into one big file:

```python
>>> r = client.request(country="FR", pl=["O3", "PM10"], year_to=2014)
>>> r.download_to_file("data/raw.csv")
```
```
Generating CSV download links...
100%|██████████| 2/2 [00:12<00:00,  7.40s/it]
Generated 2,029 CSV links ready for downloading
Writing data to data/raw.csv...
100%|██████████| 2029/2029 [31:23<00:00,  1.04it/s]
```

Download the entire dataset (not for the faint of heart):

```python
>>> r = client.request()
>>> r.download_to_directory("data")
```
```
Generating CSV download links...
100%|██████████| 40/40 [03:38<00:00,  2.29s/it]
Generated 146,993 CSV links ready for downloading
Downloading CSVs to data...
  0%|          | 299/146993 [01:50<17:15:06,  2.36it/s]
```

Don't forget to get the metadata about the measurement stations:

```python
>>> client.download_metadata("data/metadata.csv")
Writing metadata to data/metadata.csv...
```

## Roadmap

* **Tests**
* Parallel CSV downloads (in progress)
* CLI to avoid using Python all together
* Data wrangling module for AirBase output data
