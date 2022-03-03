.. AirBase documentation master file, created by
   sphinx-quickstart on Sun Mar 10 10:55:55 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to AirBase's documentation!
===================================

.. toctree::
   :maxdepth: 2

   self
   airbase

`An easy downloader for the AirBase air quality data.`

AirBase is an air quality database provided by the European Environment Agency
(EEA). The data is available for download at
`the Portal <http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm>`_, but
the interface makes it a bit time consuming to do bulk downloads. Hence, an easy
Python-based interface.



Installation
============
To install ``airbase``, simply run

.. code-block:: bash

   $ pip install airbase

``airbase`` has been tested on Python 3.7 and higher.

Quickstart
==========
Get info about available countries and pollutants:

.. code-block:: python

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

Request download links from the server and save the resulting CSVs into a directory:

.. code-block:: python

   >>> r = client.request(country=["NL", "DE"], pl="NO3", year_from=2015)
   >>> r.download_to_directory(dir="data", skip_existing=True)
   Generating CSV download links...
   100%|██████████| 2/2 [00:03<00:00,  2.03s/it]
   Generated 12 CSV links ready for downloading
   Downloading CSVs to data...
   100%|██████████| 12/12 [00:01<00:00,  8.44it/s]

Or concatenate them into one big file:

.. code-block:: python

   >>> r = client.request(country="FR", pl=["O3", "PM10"], year_to=2014)
   >>> r.download_to_file("data/raw.csv")
   Generating CSV download links...
   100%|██████████| 2/2 [00:12<00:00,  7.40s/it]
   Generated 2,029 CSV links ready for downloading
   Writing data to data/raw.csv...
   100%|██████████| 2029/2029 [31:23<00:00,  1.04it/s]

Download the entire dataset (not for the faint of heart):

.. code-block:: python

   >>> r = client.request()
   >>> r.download_to_directory("data")
   Generating CSV download links...
   100%|██████████| 40/40 [03:38<00:00,  2.29s/it]
   Generated 146,993 CSV links ready for downloading
   Downloading CSVs to data...
     0%|          | 299/146993 [01:50<17:15:06,  2.36it/s]

Don't forget to get the metadata about the measurement stations:

.. code-block:: python

   >>> client.download_metadata("data/metadata.tsv")
   Writing metadata to data/metadata.tsv...

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

By default, the request will request the entire dataset, which will take most of a
day to download. Its arguments can be used to filter to only specific dates, countries,
pollutants, etc.

``pl`` and ``shortpl``
----------------------
The common abbreviations for pollutants ("O3", "NOX", "PM10", etc.) are referred to in the
``airbase`` package as ``pl``. The AirBase Portal internally makes use of a numeric
system for labelling pollutants, which we refer to as the ``shortpl``. The client
is built in such a way as to only require knowing the familiar ``pl`` you are
looking for, but the pollutant lists and search functionality provided by
the client will always return both the ``pl`` and the ``shortpl`` for every pollutant,
as these are required for constructing the requests and communicating with the AirBase
Portal.




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
