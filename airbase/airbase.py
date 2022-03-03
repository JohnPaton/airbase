from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

from .fetch import (
    fetch_text,
    fetch_to_directory,
    fetch_to_file,
    fetch_unique_lines,
)
from .resources import CURRENT_YEAR, METADATA_URL
from .summary import DB
from .util import link_list_url, string_safe_list


class PollutantDict(TypedDict):
    pl: str
    shortpl: int


class AirbaseClient:
    def __init__(self) -> None:
        """
        The central point for requesting Airbase data.

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

        """All countries available from AirBase"""
        self.all_countries = DB.countries()

        """All pollutants available from AirBase"""
        self.all_pollutants = DB.pollutants()

        """The pollutants available in each country from AirBase."""
        self.pollutants_per_country: dict[str, list[PollutantDict]] = dict()
        for country, pollutants in DB.pollutants_per_country().items():
            self.pollutants_per_country[country] = [
                dict(pl=pl, shortpl=id) for pl, id in pollutants.items()
            ]

    def request(
        self,
        country: str | list[str] | None = None,
        pl: str | list[str] | None = None,
        shortpl: str | list[str] | None = None,
        year_from: str = "2013",
        year_to: str = CURRENT_YEAR,
        source: str = "All",
        update_date: str | datetime | None = None,
        verbose: bool = True,
        preload_csv_links: bool = False,
    ) -> AirbaseRequest:
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

        :param country: (optional), 2-letter country code or a
            list of them. If a list, data will be requested for each
            country. Will raise ValueError if a country is not available
            on the server. If None, data for all countries will be
            requested. See `self.all_countries`.
        :param pl: (optional) The pollutant(s) to request data
            for. Must be one of the pollutants in `self.all_pollutants`.
            Cannot be used in conjunction with `shortpl`.
        :param shortpl: (optional). The pollutant code(s) to
            request data for. Will be applied to each country requested.
            Cannot be used in conjunction with `pl`.
        :param year_from: (optional) The first year of data. Can
            not be earlier than 2013. Default 2013.
        :param year_to: (optional) The last year of data. Can not be
            later than the current year. Default <current year>.
        :param source: (optional) One of "E1a", "E2a" or "All". E2a
            (UTD) data are only available for years where E1a data have
            not yet been delivered (this will normally be the most
            recent year). Default "All".
        :param update_date: (optional). Format
            "yyyy-mm-dd hh:mm:ss". To be used when only files created or
            updated after a certain date is of interest.
        :param verbose: (optional) print status messages to stderr.
            Default True.
        :param preload_csv_links: (optional) Request all the csv
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
        if country is None:
            country = self.all_countries
        else:
            country = string_safe_list(country)
            self._validate_country(country)

        if pl is not None and shortpl is not None:
            raise ValueError("You cannot specify both 'pl' and 'shortpl'")

        # construct shortpl form pl if applicable
        if pl is not None:
            pl_list = string_safe_list(pl)
            try:
                shortpl = [self.all_pollutants[p] for p in pl_list]
            except KeyError as e:
                raise ValueError(
                    f"'{e.args[0]}' is not a valid pollutant name"
                ) from e

        return AirbaseRequest(
            country,
            shortpl,
            year_from,
            year_to,
            source,
            update_date,
            verbose,
            preload_csv_links,
        )

    def search_pollutant(
        self, query: str, limit: int | None = None
    ) -> list[PollutantDict]:
        """
        Search for a pollutant's `shortpl` number based on its name.

        :param query: The pollutant to search for.
        :param limit: (optional) Max number of results.

        :return: The best pollutant matches. Pollutants
            are dicts with keys "pl" and "shortpl".

        :example:
            >>> AirbaseClient().search_pollutant("o3", limit=2)
            >>> [{"pl": "O3", "shortpl": "7"}, {"pl": "NO3", "shortpl": "46"}]

        """
        results = DB.search_pollutant(query, limit=limit)
        return [dict(pl=pl, shortpl=id) for pl, id in results.items()]

    @staticmethod
    def download_metadata(filepath: str | Path, verbose: bool = True) -> None:
        """
        Download the metadata file.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param filepath:
        :param verbose:
        """
        AirbaseRequest(verbose=verbose).download_metadata(filepath)

    def _validate_country(self, country: str | list[str]) -> None:
        """
        Ensure that a country or list of countries exists on the server.

        Must first download the country list using `.connect()`. Raises
        value error if a country does not exist.

        :param country: The 2-letter country code to validate.
        """
        country_list = string_safe_list(country)
        for c in country_list:
            if c not in self.all_countries:
                raise ValueError(
                    f"'{c}' is not an available 2-letter country code."
                )


class AirbaseRequest:
    def __init__(
        self,
        country: str | list[str] | None = None,
        shortpl: str | list[str] | None = None,
        year_from: str = "2013",
        year_to: str = CURRENT_YEAR,
        source: str = "All",
        update_date: str | datetime | None = None,
        verbose: bool = True,
        preload_csv_links: bool = False,
    ) -> None:
        """
        Handler for Airbase data requests.

        Requests proceed in two steps: First, links to individual CSVs
        are requested from the Airbase server. Then these links are
        used to download the individual CSVs.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param country: 2-letter country code or a list of
            them. If a list, data will be requested for each country.
        :param shortpl: (optional). The pollutant code to
            request data for. Will be applied to each country requested.
            If None, all available pollutants will be requested. If a
            pollutant is not available for a country, then we simply
            do not try to download those CSVs.
        :param year_from: (optional) The first year of data. Can
            not be earlier than 2013. Default 2013.
        :param year_to: (optional) The last year of data. Can not be
            later than the current year. Default <current year>.
        :param source: (optional) One of "E1a", "E2a" or "All". E2a
            (UTD) data are only available for years where E1a data have
            not yet been delivered (this will normally be the most
            recent year). Default "All".
        :param update_date: (optional). Format
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

        self._country_list = string_safe_list(country)
        self._shortpl_list = string_safe_list(shortpl)
        self._download_links = []

        for c in self._country_list:
            for p in self._shortpl_list:
                self._download_links.append(
                    link_list_url(c, p, year_from, year_to, source, update_date)
                )

        self._csv_links: list[str] = []

        if preload_csv_links:
            self._get_csv_links()

    def _get_csv_links(self, force: bool = False) -> None:
        """
        Request all relevant CSV links from the server.

        This can take some time (several minutes for the entire set).
        This action will only be performed once, unless `force` is set
        to True.

        :param force: Re-download all of the links, even if they
            are already known
        """
        if self._csv_links and not force:
            return

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

    def download_to_directory(
        self,
        dir: str | Path,
        skip_existing: bool = True,
        raise_for_status: bool = True,
    ) -> AirbaseRequest:
        """
        Download into a directory, preserving original file structure.

        :param dir: The directory to save files in (must exist)
        :param skip_existing: (optional) Don't re-download files if
            they exist in `dir`. If False, existing files in `dir` may
            be overwritten. Default True.
        :param raise_for_status: (optional) Raise exceptions if
            download links return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead. Default True.

        :return: self
        """
        # ensure the directory exists
        dir = Path(dir)
        if not dir.is_dir():
            raise NotADirectoryError(f"{dir.resolve()} is not a directory.")

        self._get_csv_links()

        if self.verbose:
            print(f"Downloading CSVs to {dir}...", file=sys.stderr)

        fetch_to_directory(
            self._csv_links,
            dir,
            skip_existing=skip_existing,
            progress=self.verbose,
            raise_for_status=raise_for_status,
        )

        return self

    def download_to_file(
        self, filepath: str | Path, raise_for_status: bool = True
    ) -> AirbaseRequest:
        """
        Download data into one large CSV.

        Directory where the new CSV will be created must exist.

        :param filepath: The path to the new CSV.
        :param raise_for_status: (optional) Raise exceptions if
            download links return "bad" HTTP status codes. If False,
            a :py:func:`warnings.warn` will be issued instead. Default True.

        :return: self
        """
        self._get_csv_links()

        # ensure the path is valid
        filepath = Path(filepath)
        if not filepath.parent.is_dir():
            raise NotADirectoryError(
                f"{filepath.parent.resolve()} does not exist."
            )

        if self.verbose:
            print(f"Writing data to {filepath}...", file=sys.stderr)
        fetch_to_file(
            self._csv_links,
            filepath,
            progress=self.verbose,
            raise_for_status=raise_for_status,
        )

        return self

    def download_metadata(self, filepath: str | Path) -> None:
        """
        Download the metadata TSV file.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param filepath: Where to save the TSV
        """
        # ensure the path is valid
        filepath = Path(filepath)
        if not filepath.parent.is_dir():
            raise NotADirectoryError(
                f"{filepath.parent.resolve()} does not exist."
            )

        if self.verbose:
            print(f"Writing metadata to {filepath}...", file=sys.stderr)
        text = fetch_text(METADATA_URL)
        filepath.write_text(text)
