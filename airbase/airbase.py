import os
import sys
from pathlib import Path

from .resources import E1A_SUMMARY_URL, METADATA_URL, CURRENT_YEAR
from . import util
from .fetch import (
    fetch_json,
    fetch_text,
    fetch_to_directory,
    fetch_to_file,
    fetch_unique_lines,
)


class AirbaseClient:
    def __init__(self, connect=True):
        """
        The central point for requesting Airbase data.

        :param bool connect: (optional) Immediately test network
            connection and download available countries and pollutants.
            If False, `.connect()` must be called before making data
            requests. Default True.

        :example:
            >>> client = AirbaseClient()
            >>> r = client.request(["NL", "DE"], pl=["O3", "NO2"])
            >>> r.download_to_directory("data/raw")
            Generating CSV download links...
            100%|██████████| 4/4 [00:09<00:00,  2.64s/it]
            Generated 5164 CSV links ready for downloading
            Downloading CSVs to data/raw...
            100%|██████████| 5164/5164 [43:39<00:00,  1.95it/s]
            >>> r.download_metadata("data/metadata.tsv")
            Writing metadata to data/metadata.tsv...

        """
        self._all_countries = None
        self._all_pollutants = None
        self._pollutants_per_country = None
        self._cities_per_country = None
        self._current_request = None

        if connect:
            self.connect()

    def connect(self, timeout=None):
        """
        Download the available countries and pollutants for validation.

        :param float timeout: Raise ConnectionError if the server takes
            longer than `timeout` seconds to respond.

        :return: self
        """
        summary = fetch_json(E1A_SUMMARY_URL, timeout=timeout)
        self._all_countries = util.countries_from_summary(summary)
        self._all_pollutants = util.pollutants_from_summary(summary)
        self._pollutants_per_country = util.pollutants_per_country(summary)

        return self

    def request(
        self,
        country=None,
        pl=None,
        shortpl=None,
        year_from="2013",
        year_to=CURRENT_YEAR,
        source="All",
        update_date=None,
        verbose=True,
        preload_csv_links=False,
    ):
        """
        Initialize an AirbaseRequest for a query.

        Pollutants can be specified either by name (`pl`) or by code
        (`shortpl`). If no pollutants are specified, data for all
        available pollutants will be requested. If a pollutant is not
        available for a country, then we simply do not try to download
        those CSVs.

        Requests proceed in two steps: First, links to individual CSVs
        are requested from the Airbase server. Then these links are
        used to download the individual CSVs.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param str|list country: (optional), 2-letter country code or a
            list of them. If a list, data will be requested for each
            country. Will raise ValueError if a country is not available
            on the server. If None, data for all countries will be
            requested. See `self.all_countries`.
        :param str|list pl: (optional) The pollutant(s) to request data
            for. Must be one of the pollutants in `self.all_pollutants`.
            Cannot be used in conjunction with `shortpl`.
        :param str|list shortpl: (optional). The pollutant code(s) to
            request data for. Will be applied to each country requested.
            Cannot be used in conjunction with `pl`.
        :param str year_from: (optional) The first year of data. Can
            not be earlier than 2013. Default 2013.
        :param str year_to: (optional) The last year of data. Can not be
            later than the current year. Default <current year>.
        :param str source: (optional) One of "E1a", "E2a" or "All". E2a
            (UTD) data are only available for years where E1a data have
            not yet been delivered (this will normally be the most
            recent year). Default "All".
        :param str|datetime update_date: (optional). Format
            "yyyy-mm-dd hh:mm:ss". To be used when only files created or
            updated after a certain date is of interest.
        :param bool verbose: (optional) print status messages to stderr.
            Default True.
        :param bool preload_csv_links: (optional) Request all the csv
            download links from the Airbase server at object
            initialization. Default False.

        :return AirbaseRequest:
            The initialized AirbaseRequest.

        :example:
            >>> client = AirbaseClient()
            >>> r = client.request(["NL", "DE"], pl=["O3", "NO2"])
            >>> r.download_to_directory("data/raw")
            Generating CSV download links...
            100%|██████████| 4/4 [00:09<00:00,  2.64s/it]
            Generated 5164 CSV links ready for downloading
            Downloading CSVs to data/raw...
            100%|██████████| 5164/5164 [43:39<00:00,  1.95it/s]
            >>> r.download_metadata("data/metadata.tsv")
            Writing metadata to data/metadata.tsv...
        """
        # validation
        if country:
            country = util.string_safe_list(country)
            self._validate_country(country)
        else:
            country = self.all_countries

        if pl is not None and shortpl is not None:
            raise ValueError("You cannot specify both 'pl' and 'shortpl'")

        # construct shortpl form pl if applicable
        if pl is not None:
            pl_list = util.string_safe_list(pl)
            shortpl = []
            for p in pl_list:
                try:
                    shortpl.append(self.all_pollutants[p])
                except KeyError:
                    raise ValueError(
                        "'{}' is not a valid pollutant name".format(p)
                    )

        r = AirbaseRequest(
            country,
            shortpl,
            year_from,
            year_to,
            source,
            update_date,
            verbose,
            preload_csv_links,
        )

        self._current_request = r
        return r

    def search_pollutant(self, query, limit=None):
        """
        Search for a pollutant's `shortpl` number based on its name.

        :param str query: The pollutant to search for.
        :param int limit: (optional) Max number of results.

        :return list[dict]: The best pollutant matches. Pollutants
            are dicts with keys "pl" and "shortpl".

        :example:
            >>> AirbaseClient().search_pollutant("o3", limit=2)
            >>> [{"pl": "O3", "shortpl": "7"}, {"pl": "NO3", "shortpl": "46"}]

        """
        names = list(self.all_pollutants.keys())
        # substring search
        results = [n for n in names if query.lower() in n.lower()]

        # shortest results first
        results.sort(key=lambda x: len(x))

        if limit:
            results = results[:limit]

        return [
            {"pl": name, "shortpl": self.all_pollutants[name]}
            for name in results
        ]

    @staticmethod
    def download_metadata(filepath, verbose=True):
        """
        Download the metadata file.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param str filepath:
        :param bool verbose:
        """
        AirbaseRequest(verbose=verbose).download_metadata(filepath)

    def _validate_country(self, country):
        """
        Ensure that a country or list of countries exists on the server.

        Must first download the country list using `.connect()`. Raises
        value error if a country does not exist.

        :param str|list country: The 2-letter country code to validate.
        """
        country_list = util.string_safe_list(country)
        for c in country_list:
            if c not in self.all_countries:
                raise ValueError(
                    "'{}' is not an available 2-letter country code.".format(c)
                )

    @property
    def all_countries(self):
        """All countries available from AirBase."""
        if self._all_countries is None:
            raise AttributeError(
                "Country list has not yet been downloaded. "
                "Please .connect() first."
            )
        return self._all_countries

    @property
    def all_pollutants(self):
        """All pollutants available from AirBase."""
        if self._all_pollutants is None:
            raise AttributeError(
                "Pollutant list has not yet been downloaded. "
                "Please .connect() first."
            )
        return self._all_pollutants

    @property
    def pollutants_per_country(self):
        """The pollutants available in each country from AirBase."""
        if self._pollutants_per_country is None:
            raise AttributeError(
                "Country-Pollutant map has not yet been downloaded. "
                "Please .connect() first."
            )
        return self._pollutants_per_country


class AirbaseRequest:
    def __init__(
        self,
        country=None,
        shortpl=None,
        year_from="2013",
        year_to=CURRENT_YEAR,
        source="All",
        update_date=None,
        verbose=True,
        preload_csv_links=False,
    ):
        """
        Handler for Airbase data requests.

        Requests proceed in two steps: First, links to individual CSVs
        are requested from the Airbase server. Then these links are
        used to download the individual CSVs.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param str|list country: 2-letter country code or a list of
            them. If a list, data will be requested for each country.
        :param str|list shortpl: (optional). The pollutant code to
            request data for. Will be applied to each country requested.
            If None, all available pollutants will be requested. If a
            pollutant is not available for a country, then we simply
            do not try to download those CSVs.
        :param str year_from: (optional) The first year of data. Can
            not be earlier than 2013. Default 2013.
        :param str year_to: (optional) The last year of data. Can not be
            later than the current year. Default <current year>.
        :param str source: (optional) One of "E1a", "E2a" or "All". E2a
            (UTD) data are only available for years where E1a data have
            not yet been delivered (this will normally be the most
            recent year). Default "All".
        :param str|datetime update_date: (optional). Format
            "yyyy-mm-dd hh:mm:ss". To be used when only files created or
            updated after a certain date is of interest.
        :param bool verbose: (optional) print status messages to stderr.
            Default True.
        :param bool preload_csv_links: (optional) Request all the csv
            download links from the Airbase server at object
            initialization. Default False.
        """
        self.country = country
        self.shortpl = shortpl
        self.year_from = year_from
        self.year_to = year_to
        self.source = source
        self.update_date = update_date
        self.verbose = verbose

        self._country_list = util.string_safe_list(country)
        self._shortpl_list = util.string_safe_list(shortpl)
        self._download_links = []

        for c in self._country_list:
            for p in self._shortpl_list:
                self._download_links.append(
                    util.link_list_url(
                        c, p, year_from, year_to, source, update_date
                    )
                )

        self._csv_links = []

        if preload_csv_links:
            self._get_csv_links()

    def _get_csv_links(self, force=False):
        """
        Request all relevant CSV links from the server.

        This can take some time (several minutes for the entire set).
        This action will only be performed once, unless `force` is set
        to True.

        :param bool force: Re-download all of the links, even if they
            are already known

        :return: self
        """
        if self._csv_links and not force:
            return self._csv_links

        if self.verbose:
            print("Generating CSV download links...", file=sys.stderr)

        # set of links (no duplicates)
        csv_links = fetch_unique_lines(
            self._download_links,
            progress=self.verbose,
            encoding="utf-8-sig",
        )

        # list of links (no duplicates)
        self._csv_links = list(csv_links)

        if self.verbose:
            print(
                "Generated {:,} CSV links ready for downloading".format(
                    len(self._csv_links)
                ),
                file=sys.stderr,
            )

        return self

    def download_to_directory(
        self, dir, skip_existing=True, raise_for_status=True
    ):
        """
        Download into a directory, preserving original file structure.

        :param str dir: The directory to save files in (must exist)
        :param bool skip_existing: (optional) Don't re-download files if
            they exist in `dir`. If False, existing files in `dir` may
            be overwritten. Default True.
        :param bool raise_for_status: (optional) Raise exceptions if
            download links return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead. Default True.

        :return: self
        """
        # ensure the directory exists
        if not os.path.isdir(dir):
            raise NotADirectoryError(
                os.path.realpath(dir) + " is not a directory."
            )

        self._get_csv_links()

        if self.verbose:
            print("Downloading CSVs to {}...".format(dir), file=sys.stderr)

        fetch_to_directory(
            self._csv_links,
            Path(dir),
            skip_existing=skip_existing,
            progress=self.verbose,
            raise_for_status=raise_for_status,
        )

        return self

    def download_to_file(self, filepath, raise_for_status=True):
        """
        Download data into one large CSV.

        Directory where the new CSV will be created must exist.

        :param str filepath: The path to the new CSV.
        :param bool raise_for_status: (optional) Raise exceptions if
            download links return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead. Default True.

        :return: self
        """
        self._get_csv_links()

        if self.verbose:
            print("Writing data to {}...".format(filepath), file=sys.stderr)

        # ensure the path is valid
        if not os.path.exists(os.path.dirname(os.path.realpath(filepath))):
            raise NotADirectoryError(
                os.path.dirname(os.path.realpath(filepath)) + " does not exist."
            )

        fetch_to_file(
            self._csv_links,
            Path(filepath),
            progress=self.verbose,
            raise_for_status=raise_for_status,
        )

        return self

    def download_metadata(self, filepath):
        """
        Download the metadata TSV file.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param str filepath: Where to save the TSV
        """
        # ensure the path is valid
        if not os.path.exists(os.path.dirname(os.path.realpath(filepath))):
            raise NotADirectoryError(
                os.path.dirname(filepath) + " does not exist."
            )

        if self.verbose:
            print("Writing metadata to {}...".format(filepath), file=sys.stderr)

        text = fetch_text(METADATA_URL)
        Path(filepath).write_text(text)
