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

Air quality data in in CSV format
---------------------------------

.. code-block:: console

   $ airbase download --help
   Usage: airbase download [OPTIONS]

     Air quality data in in CSV format. **End of life 2024**.

     The service providing air quality data in CSV format will cease operations by the end of 2024.
     Until then it will provide only **unverified** data (E2a) for 2024.

     Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
     - download only Norwegian, Danish and Finish sites
       airbase download -c NO -c DK -c FI
     - download only SO2, PM10 and PM2.5 observations
       airbase download -p SO2 -p PM10 -p PM2.5

     Use -C/--city to further restrict the download to specific cities, e.g.
     - download only PM10 and PM2.5 from Valletta, the Capital of Malta
       airbase download -C Valletta -c MT -p PM10 -p PM2.5

   Options:
     -c, --country [AD|AL|AT|...]
     -p, --pollutant [k|V|TI|...]
     -C, --city TEXT                 only from selected <cities>
     -M, --metadata                  download station metadata
     --path PATH                     [default: data]
     --year INTEGER RANGE            The service providing air quality data in CSV format will cease operations by the end of 2024.
                                     Until then it will provide only **unverified** data (E2a) for 2024.  [default: 2024; 2024<=x<=2024]
     -O, --overwrite                 Re-download existing files.
     -q, --quiet                     No progress-bar.
     --help                          Show this message and exit.

Historical data delivered between 2002 and 2012
-----------------------------------------------

.. code-block:: console

   $ airbase historical --help
   Usage: airbase historical [OPTIONS]

     Historical Airbase data delivered between 2002 and 2012 before Air Quality
     Directive 2008/50/EC entered into force.

     Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
     - download only Norwegian, Danish and Finish sites
       airbase download -c NO -c DK -c FI
     - download only SO2, PM10 and PM2.5 observations
       airbase download -p SO2 -p PM10 -p PM2.5

     Use -C/--city to further restrict the download to specific cities, e.g.
     - download only PM10 and PM2.5 from Valletta, the Capital of Malta
       airbase download -C Valletta -c MT -p PM10 -p PM2.5

   Options:
     -c, --country [AD|AL|AT|...]
     -p, --pollutant [k|V|NT|...]
     -C, --city TEXT                 only from selected <cities>
     -F, --aggregation-type, --frequency [hourly|daily|other]
                                     only hourly data, daily data or other
                                     aggregation frequency
     -M, --metadata                  download station metadata
     --path PATH                     [default: data/historical]
     -n, --dry-run, --summary        Total download files/size, nothing will be
                                     downloaded.
     -O, --overwrite                 Re-download existing files.
     -q, --quiet                     No progress-bar.
     --help                          Show this message and exit.


Verified data from 2013 to 2023
-------------------------------
.. code-block:: console
   $ airbase verified --help
   Usage: airbase verified [OPTIONS]

     Verified data (E1a) from 2013 to 2023 reported by countries by 30 September
     each year for the previous year.

     Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
     - download only Norwegian, Danish and Finish sites
       airbase download -c NO -c DK -c FI
     - download only SO2, PM10 and PM2.5 observations
       airbase download -p SO2 -p PM10 -p PM2.5

     Use -C/--city to further restrict the download to specific cities, e.g.
     - download only PM10 and PM2.5 from Valletta, the Capital of Malta
       airbase download -C Valletta -c MT -p PM10 -p PM2.5

   Options:
     -c, --country [AD|AL|AT|...]
     -p, --pollutant [k|V|NT|...]
     -C, --city TEXT                 only from selected <cities>
     -F, --aggregation-type, --frequency [hourly|daily|other]
                                     only hourly data, daily data or other
                                     aggregation frequency
     -M, --metadata                  download station metadata
     --path PATH                     [default: data/verified]
     -n, --dry-run, --summary        Total download files/size, nothing will be
                                     downloaded.
     -O, --overwrite                 Re-download existing files.
     -q, --quiet                     No progress-bar.
     --help                          Show this message and exit.

Unverified data from the beginning of 2024
------------------------------------------
.. code-block:: console
   $ airbase unverified --help
   Usage: airbase unverified [OPTIONS]

     Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the
     beginning of 2024.

     Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants, e.g.
     - download only Norwegian, Danish and Finish sites
       airbase download -c NO -c DK -c FI
     - download only SO2, PM10 and PM2.5 observations
       airbase download -p SO2 -p PM10 -p PM2.5

     Use -C/--city to further restrict the download to specific cities, e.g.
     - download only PM10 and PM2.5 from Valletta, the Capital of Malta
       airbase download -C Valletta -c MT -p PM10 -p PM2.5

   Options:
     -c, --country [AD|AL|AT|...]
     -p, --pollutant [k|V|NT|...]
     -C, --city TEXT                 only from selected <cities>
     -F, --aggregation-type, --frequency [hourly|daily|other]
                                     only hourly data, daily data or other
                                     aggregation frequency
     -M, --metadata                  download station metadata
     --path PATH                     [default: data/unverified]
     -n, --dry-run, --summary        Total download files/size, nothing will be
                                     downloaded.
     -O, --overwrite                 Re-download existing files.
     -q, --quiet                     No progress-bar.
     --help                          Show this message and exit.

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
