from __future__ import annotations

from collections.abc import Iterable, Iterator
from itertools import islice
from pathlib import Path
from warnings import warn

import polars as pl  # type:ignore[import-not-found]

from airbase.summary import DB

STATION_TYPE = (  # http://dd.eionet.europa.eu/vocabulary/aq/stationclassification/
    "background",
    "industrial",
    "traffic",
)


STATION_AREA = (  # http://dd.eionet.europa.eu/vocabulary/aq/areaclassification/
    "rural",
    "rural-nearcity",
    "rural-regional",
    "rural-remote",
    "suburban",
    "urban",
)

AGGREGATION_TYPE = (  # https://eeadmz1-downloads-webapp.azurewebsites.net/content/documentation/How_To_Downloads.pdf
    "hour",
    "day",
    "var",
)


def station_metadata(path: Path) -> pl.DataFrame:
    country_name = {
        "Türkiye": "Turkey",
        "Kosovo under UNSCR 1244/99": "Kosovo",
    }
    country_code = DB.COUNTRY_CODE | {
        "Türkiye": DB.COUNTRY_CODE["Turkey"],
        "Kosovo under UNSCR 1244/99": DB.COUNTRY_CODE["Kosovo"],
        "Ukraine": DB.COUNTRY_CODE.get("Ukraine", "UA"),
        "Georgia": DB.COUNTRY_CODE.get("Georgia", "GE"),
    }
    time_zone = {
        "UTC-04": "Etc/GMT-4",
        "UTC-03": "Etc/GMT-3",
        "UTC-02": "Etc/GMT-2",
        "UTC-01": "Etc/GMT-1",
        "UTC": "UTC",
        "UTC+01": "Etc/GMT+1",
        "UTC+02": "Etc/GMT+2",
        "UTC+03": "Etc/GMT+3",
        "UTC+04": "Etc/GMT+4",
    }

    return (
        pl.scan_csv(
            path,
            schema_overrides={
                "Longitude": pl.Float64,
                "Latitude": pl.Float64,
                "Altitude": pl.Float32,
            },
        )
        .unique(
            ("Sampling Point Id", "Air Pollutant"),
            keep="last",
        )
        .select(
            pl.col("Country").replace(country_name),
            pl.col("Country")
            .replace_strict(country_code)
            .alias("Country Code"),
            pl.col("Timezone").str.to_uppercase().replace_strict(time_zone),
            "Air Quality Station EoI Code",
            pl.col("Air Quality Station Name").str.strip_chars('"'),
            "Sampling Point Id",
            "Air Pollutant",
            "Longitude",
            "Latitude",
            "Altitude",
            "Air Quality Station Type",
            "Air Quality Station Area",
        )
        .collect()
    )


def obs_time_range(
    paths: Iterable[Path], *, exclude: set[str] = set()
) -> Iterator[pl.DataFrame]:
    # https://dd.eionet.europa.eu/vocabulary/aq/observationvalidity
    # -99 Not valid due to station maintenance or calibration
    #  -1 Not valid
    #   1 Valid
    #   2 Valid, but below detection limit measurement value given
    #   3 Valid, but below detection limit and number replaced by 0.5*detection limit
    #   4 Valid (Ozone only) using CCQM.O3.2019
    validity = pl.col("Validity").is_in({1, 2, 3, 4})

    # https://dd.eionet.europa.eu/vocabulary/aq/observationverification
    #   1 Verified
    #   2 Preliminary verified
    #   3 Not verified
    verification = pl.col("Verification").is_in({1, 2, 3})

    for path in paths:
        if path.stem.casefold() in exclude:
            continue

        df = (
            pl.scan_parquet(path, include_file_paths="filename")
            .filter(
                validity,
                verification,
            )
            .group_by(
                "filename",
                "Samplingpoint",
                "AggType",
            )
            .agg(
                pl.min("Start"),
                pl.max("End"),
            )
        )

        try:
            yield df.collect()
        except (
            pl.exceptions.SchemaError,
            pl.exceptions.ColumnNotFoundError,
            pl.exceptions.DuplicateError,
        ) as e:
            warn(f"{e} while reading {path}, skip")


def catalog(
    data_path: Path,
    metadata: Path,
    *,
    exclude: set[str] = {"catalog", "metadata"},
    stop_after: int | None = None,
) -> pl.DataFrame:
    """
    Combine station and observation metadata from all observation files on `data_path`
    """

    df = (
        pl.concat(
            islice(
                obs_time_range(data_path.rglob("*.parquet"), exclude=exclude),
                stop_after,
            )
        )
        .join(
            station_metadata(metadata),
            how="left",
            left_on="Samplingpoint",
            right_on=pl.format("{}/{}", "Country Code", "Sampling Point Id"),
        )
        .sort("Samplingpoint", "AggType")
    )

    missing = df.filter(pl.any_horizontal(pl.all().is_null()))
    if not missing.is_empty():
        for file in missing.get_column("filename"):
            warn(f"No metadata for {data_path}/{file}")

    # Start/End datatype has no time zome info
    # use UTC+01 for AggType=hour/var and Timezone for AggType=day
    # https://eeadmz1-downloads-webapp.azurewebsites.net/
    # ┌────────┬───────────┬────────────┐
    # │ old_tz ┆ new_tz    ┆ utc_offset │
    # │ ---    ┆ ---       ┆ ---        │
    # │ str    ┆ str       ┆ str        │
    # ╞════════╪═══════════╪════════════╡
    # │ UTC-04 ┆ Etc/GMT-4 ┆ 4h         │
    # │ UTC-03 ┆ Etc/GMT-3 ┆ 3h         │
    # │ UTC-02 ┆ Etc/GMT-2 ┆ 2h         │
    # │ UTC-01 ┆ Etc/GMT-1 ┆ 1h         │
    # │ UTC    ┆ UTC       ┆ 0h         │
    # │ UTC+01 ┆ Etc/GMT+1 ┆ -1h        │
    # │ UTC+02 ┆ Etc/GMT+2 ┆ -2h        │
    # │ UTC+03 ┆ Etc/GMT+3 ┆ -3h        │
    # │ UTC+04 ┆ Etc/GMT+4 ┆ -4h        │
    # └────────┴───────────┴────────────┘
    time_zone = (
        pl.when(AggType="day").then("Timezone").otherwise(pl.lit("Etc/GMT+1"))
    )
    utc_offset = time_zone.replace_strict(
        {
            "Etc/GMT-4": "4h",
            "Etc/GMT-3": "3h",
            "Etc/GMT-2": "2h",
            "Etc/GMT-1": "1h",
            "UTC": "0h",
            "Etc/GMT+1": "-1h",
            "Etc/GMT+2": "-2h",
            "Etc/GMT+3": "-3",
            "Etc/GMT+4": "-4h",
        }
    )

    return df.drop_nulls().select(
        pl.col("filename").str.slice(len(f"{data_path}/")),
        pl.col("Country").cast(pl.Enum(DB.COUNTRY_CODE.keys())),
        pl.col("Country Code").cast(pl.Enum(DB.COUNTRY_CODE.values())),
        "Air Quality Station EoI Code",
        "Air Quality Station Name",
        "Sampling Point Id",
        pl.col("Air Pollutant").cast(pl.Categorical),
        "Longitude",
        "Latitude",
        "Altitude",
        pl.col("Air Quality Station Type").cast(pl.Enum(STATION_TYPE)),
        pl.col("Air Quality Station Area").cast(pl.Enum(STATION_AREA)),
        pl.col("AggType").cast(pl.Enum(AGGREGATION_TYPE)),
        time_zone.cast(pl.Categorical),
        pl.col("Start", "End")
        .dt.replace_time_zone("UTC")
        .dt.offset_by(utc_offset),
    )


def write_catalog(
    path: Path,
    data_path: Path,
    metadata: Path,
    *,
    overwrite: bool = False,
    stop_after: int | None = None,
) -> None:
    """Write combined statnon and observation metadata"""
    if not overwrite and path.is_file():
        print(f"found {path}, skip")
        return

    df = catalog(
        data_path,
        metadata,
        exclude={"catalog", path.stem, "metadata", metadata.stem},
        stop_after=stop_after,
    )
    if df.is_empty():
        warn(f"No valid observation files found on {data_path}")
        return

    df.write_parquet(path)
