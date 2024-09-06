from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Iterable, Literal, TypedDict

from .fetch import fetch_text
from .parquet_api import Dataset, Session, download
from .resources import METADATA_URL
from .summary import DB
from .util import string_safe_list


class PollutantDict(TypedDict):
    poll: str
    id: int


class AirbaseClient:
    def __init__(self) -> None:
        """
        The central point for requesting Airbase data.

        :example:
            >>> client = AirbaseClient()
            >>> r = client.request("Historical", "NL", "DE", poll=["O3", "NO2"])
            >>> r.download_to_directory("data/raw")
            generate: 100%|██████████| 4/4 [00:09<00:00,  2.64s/it]
            download: 100%|██████████| 1799/1799 [43:39<00:00,  1.95it/s]
            >>> r.download_metadata("data/metadata.tsv")
            Writing metadata to data/metadata.tsv...
        """

        """All countries available from AirBase"""
        self.countries = DB.COUNTRY_CODES

        """All pollutants available from AirBase"""
        self.pollutants = DB.POLLUTANTS

    def request(
        self,
        source: Literal["Historical", "Verified", "Unverified"] | Dataset,
        *countries: str,
        poll: str | Iterable[str] | None = None,
        verbose: bool = True,
    ) -> AirbaseRequest:
        """
        Initialize an AirbaseRequest for a query.

        Pollutants can be specified by name/notation (`poll`).
        If no pollutants are specified, data for all
        available pollutants will be requested. If a poll is not
        available for a country, then we simply do not try to download
        those parquet files.

        Requests proceed in two steps: First, URLs to individual parquet files
        are requested from the EEA server. Then these links are
        used to download the individual parquet files.

        See https://eeadmz1-downloads-webapp.azurewebsites.net/

        :param source: One of 3 options. `"Historical"` data delivered
            between 2002 and 2012, before Air Quality Directive 2008/50/EC entered into force.
            `"Verified"` data (E1a) from 2013 to 2022 reported by countries
            by 30 September each year for the previous year.
            `"Unverified"` data transmitted continuously (Up-To-Date/UTD/E2a),
            from the beginning of 2023.
        :param countries: (optional), 2-letter country codes.
            Data will be requested for each country.
            Will raise ValueError if a country is not in `self.countries`.
            If no countries are provided, data for all countries will be
            requested.
        :param poll: (optional) pollutant(s) to request data
            for. Must be one of the pollutants in `self.pollutants`.
        :param verbose: (optional) print status messages to stderr.
            Default True.
        :param preload_urls: (optional) Request all the file URLs
            from the EEA server at object initialization. Default False.

        :return AirbaseRequest:
            The initialized AirbaseRequest.

        :example:
            >>> client = AirbaseClient()
            >>> r = client.request("Historical", "NL", "DE", poll=["O3", "NO2"])
            >>> r.download_to_directory("data/raw")
            generate: 100%|██████████| 4/4 [00:09<00:00,  2.64s/it]
            download: 100%|██████████| 1799/1799 [43:39<00:00,  1.95it/s]
            >>> r.download_metadata("data/metadata.tsv")
            Writing metadata to data/metadata.tsv...
        """
        # country validation
        if not countries:
            countries = tuple(self.countries)
        else:
            countries = string_safe_list(countries)
            unknown = sorted(set(countries) - self.countries)
            if unknown:
                raise ValueError(
                    f"Unknown country code(s) {', '.join(unknown)}."
                )

        # poll validation
        if isinstance(poll, str) and poll not in self.pollutants:
            raise ValueError(f"'{poll}' is not a valid pollutant name")
        if isinstance(poll, list):
            unknown = sorted(set(poll) - self.pollutants)
            if unknown:
                raise ValueError(
                    f"Unknown pollutant name(s) {', '.join(unknown)}."
                )

        # source validation
        if isinstance(source, str):
            try:
                source = Dataset[source]
            except KeyError as e:  # pragma: no cover
                raise ValueError(
                    f"'{e.args[0]}' is not a valid source name"
                ) from e

        return AirbaseRequest(source, *countries, poll=poll, verbose=verbose)

    def search_pollutant(
        self, query: str, limit: int | None = None
    ) -> list[PollutantDict]:
        """
        Search for a pollutant's `id` number based on its name.

        :param query: The pollutant to search for.
        :param limit: (optional) Max number of results.

        :return: The best pollutant matches. Pollutants
            are dicts with keys "poll" and "id".

        :example:
            >>> AirbaseClient().search_pollutant("o3", limit=2)
            >>> [{"poll": "O3", "id": 7}, {"poll": "NO3", "id": 46}]

        """
        results = DB.search_pollutant(query, limit=limit)
        return [dict(poll=poll.notation, id=poll.id) for poll in results]

    @staticmethod
    def download_metadata(filepath: str | Path, verbose: bool = True) -> None:
        """
        Download the metadata file.

        See http://discomap.eea.europa.eu/map/fme/AirQualityExport.htm.

        :param filepath:
        :param verbose:
        """
        AirbaseRequest(
            Dataset.Historical,
            verbose=verbose,
        ).download_metadata(filepath)


class AirbaseRequest:
    session = Session()

    def __init__(
        self,
        source: Dataset,
        *country: str,
        poll: str | Iterable[str] | None = None,
        verbose: bool = True,
    ) -> None:
        """
        Handler for Airbase data requests.

        Requests proceed in two steps: First, URLs to individual parquet files
        are requested from the EEA server. Then these links are
        used to download the individual parquet files.

        See https://eeadmz1-downloads-webapp.azurewebsites.net/

        :param source: One of 3 options. `airbase.Dataset.Historical` data delivered
            between 2002 and 2012, before Air Quality Directive 2008/50/EC entered into force.
            `airbase.Dataset.Verified` data (E1a) from 2013 to 2022 reported by countries
            by 30 September each year for the previous year.
            `airbase.Dataset.Unverified` data transmitted continuously (Up-To-Date/UTD/E2a),
            from the beginning of 2023.
        :param country: 2-letter country code or a list of
            them. If a list, data will be requested for each country.
        :param poll: (optional) pollutant(s) to request data
            for. Will be applied to each country requested.
            If None, all available pollutants will be requested.
        :param bool verbose: (optional) print status messages to stderr.
            Default True.
        :param bool preload_urls: (optional) Request all the csv
            download links from the Airbase server at object
            initialization. Default False.
        """
        self.source = source
        self.counties = set(country)
        self.pollutants = set(string_safe_list(poll))
        self.verbose = verbose

    def download(
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

        asyncio.run(
            download(
                self.source,
                dir,
                countries=self.counties,
                pollutants=self.pollutants,
                overwrite=not skip_existing,
                quiet=not self.verbose,
                raise_for_status=raise_for_status,
            )
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
