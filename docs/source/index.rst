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

     Use -c/--country and -p/--pollutant to restrict the download specific countries and pollutants,
     or -C/--city and -p/--pollutant to restrict the download specific cities and pollutants, e.g.
     - download only Norwegian, Danish and Finish sites Historical Airbase dataset (2002 to 2012)
       $ airbase -c NO -c DK -c FI historical
     - download only SO2, PM10 and PM2.5 observations from the Verified E1a dataset (2013 to 2024)
       $ airbase -p SO2 -p PM10 -p PM2.5 verified
     - download only PM10 and PM2.5 from sites in Oslo from the Unverified E2a dataset (from 2025)
       $ airbase -C Oslo -p PM10 -p PM2.5 unverified

     Chain commands to request data from different datasets
     - request an estimate of the number of files and disk size required to download all
       available observations
       $ airbase --summary --quiet historical verified unverified
     - download verified and unverified PM10 and PM2.5 observations from sites in Berlin
       $ airbase -C Berlin -p PM10 verified unverified

   Options:
     -V, --version
     -c, --country [AD|AL|AT|BA|BE|BG|CH|CY|CZ|DE|DK|EE|ES|FI|FR|...]
     -p, --pollutant [V|k|Fe|As|CO|NT|TP|TI|pH|Pb|O3|Hg|H+|OC|BS|...]
     -C, --city TEXT                 Only from selected <cities> (--country
                                     option will be ignored).
     --path, --root-path PATH        Donwload root path.  [default: data]
     --subdir / --no-subdir          Download observations to
                                     PATH/dataset/country.  [default:
                                     subdir]
     -M, --metadata                  Download station metadata to
                                     PATH/metadata.csv.
     -n, --dry-run, --summary        Total download files/size, nothing will be
                                     downloaded.
     -O, --overwrite                 Re-download existing files.
     -q, --quiet                     No progress-bar.
     --help                          Show this message and exit.

   Commands:
     historical  Historical Airbase data delivered between 2002 and 2012...
     unverified  Unverified data transmitted continuously...
     verified    Verified data (E1a) from 2013 to 2024 reported by countries...


Historical data delivered between 2002 and 2012
-----------------------------------------------

.. code-block:: console

   $ airbase historical --help
   Usage: airbase historical [OPTIONS]

     Historical Airbase data delivered between 2002 and 2012 before Air Quality
     Directive 2008/50/EC entered into force.

   Options:
     --path, --data-path PATH/dataset/
                                     Override dataset donwload path.
     --help                          Show this message and exit.


Verified data from 2013 to 2024
-------------------------------
.. code-block:: console
   $ airbase verified --help
   Usage: airbase verified [OPTIONS]

     Verified data (E1a) from 2013 to 2024 reported by countries by 30 September
     each year for the previous year.

   Options:
     --path, --data-path PATH/dataset/
                                     Override dataset donwload path.
     --help                          Show this message and exit.


Unverified data from the beginning of 2025
------------------------------------------
.. code-block:: console
   $ airbase unverified --help
   Usage: airbase unverified [OPTIONS]

     Unverified data transmitted continuously (Up-To-Date/UTD/E2a) data from the
     beginning of 2025.

   Options:
     --path, --data-path PATH/dataset/
                                     Override dataset donwload path.
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
