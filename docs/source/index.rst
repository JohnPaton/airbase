.. AirBase documentation master file, created by
   sphinx-quickstart on Sun Mar 10 10:55:55 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to AirBase's documentation!
===================================

.. toctree::
   :maxdepth: 2

   self
   api/airbase

`An easy downloader for the AirBase air quality data.`

AirBase is an air quality database provided by the European Environment Agency
(EEA). The data is available for download at
`the Portal <http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm>`_, but
the interface makes it a bit time consuming to do bulk downloads. Hence, an easy
Python-based interface and accompanying CLI.


Installation
============
To install ``airbase``, simply run

.. code-block:: bash

   $ pip install airbase

``airbase`` has been tested on Python 3.7 and higher.

🚀 Getting Started
=================
🗺 Get info about available countries and pollutants:

.. code-block:: pycon

   >>> import airbase
   >>> client = airbase.AirbaseClient()
   >>> client.countries
   frozenset({'LI', 'CY', 'IE', 'LV', 'BE', 'EE', ...})

   >>> client.pollutants
   frozenset({'Co', 'sum-PCB', 'PCB-26', 'HNO3', ...})


   >>> client.search_pollutant("O3")
   [{'poll': 'O3', 'id': 7}, {'poll': 'NO3', 'id': 46}, ...]

🗂 Request download links from the server and save the resulting Parquet files into a directory:

.. code-block:: pycon

   >>> r = client.request("Verified", "NL", "DE", poll=["NO3", "NO3- in PM2.5", "NO3- in PM10"])
   >>> r.download(dir="data", skip_existing=True)
   summary : 100%|█████| 2/2 [00:00<00:00,  4.48requests/s]
   URLs    : 100%|█████| 29.0/29.0 [00:00<00:00, 490URL/s]
   download: 386kb [00:00, 570kb/s]

🌡 Don't forget to get the metadata about the measurement stations:

.. code-block:: pycon

   >>> client.download_metadata("data/metadata.csv")
   Writing metadata to data/metadata.csv...


🚆 Command line interface
=========================

.. code-block:: console

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
     into different (existing) paths in order to avoid filename collisions
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
   metadata    Download station metadata.


Historical data delivered between 2002 and 2012
-----------------------------------------------

.. code-block:: console

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


Verified data from 2013 to 2023
-------------------------------
.. code-block:: console

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

Unverified data from the beginning of 2024
------------------------------------------
.. code-block:: console

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

Station metadata
----------------
.. code-block:: console

   $ airbase metadata --help
   Usage: airbase metadata [OPTIONS]

     Download station metadata.

   Use chan notation to download metadata and observations, e.g.
   - download station metadata and hourly PM10 and PM2.5 observations
     from sites in Oslo into into different paths
       airbase --quiet --no-subdir \
         historical --path data/historical -F hourly -p PM10 -p PM2.5 -C Oslo \
         verified   --path data/verified   -F hourly -p PM10 -p PM2.5 -C Oslo \
         unverified --path data/unverified -F hourly -p PM10 -p PM2.5 -C Oslo \
         metadata   --path data/

   Options:
     --path PATH  [default: data]
     --metadata   Station metadata. [default: PATH/metadata.csv]
     --help       Show this message and exit.

Key Concepts
============
The ``airbase`` package is centered around two key objects: the ``AirbaseClient``
and the ``AirbaseRequest``.

``AirbaseClient``
-----------------
**The client** is responsible for generating and validating
requests. It does this by gathering information from the AirBase Portal when it is
initialized, allowing it to know which countries and pollutants are currently
available.

``AirbaseRequest``
------------------
**The request** is an object that is generally created using the
``AirbaseClient.request`` method. The request automatically handles the 2-step process
of generating CSV links for your query using the AirBase Portal, and downloading the
resulting list of CSVs. All that the user needs to do is choose where the downloaded
CSVs should be saved, and whether they should stay seperate or get concatenated
into one big file.

By default, the request will request the entire selected dataset, which will take most of a
day to download. Its arguments can be used to filter to only specific dates, countries,
pollutants, etc.



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
