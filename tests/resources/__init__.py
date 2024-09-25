import sys

if sys.version_info >= (3, 11):
    from importlib import resources
else:
    import importlib_resources as resources

# Legacy CSV API
LEGACY_CSV_URLS_RESPONSE: str
LEGACY_METADATA_RESPONSE: str

# Parquet downloads API
JSON_DOWNLOAD_SUMMARY_RESPONSE: str
CSV_PARQUET_URLS_RESPONSE: str
ZIP_CSV_METADATA_RESPONSE: bytes


def __getattr__(name: str):
    text_response = dict(
        LEGACY_CSV_URLS_RESPONSE="Legacy_MT_SO2.csv",
        LEGACY_METADATA_RESPONSE="Legacy_metadata.tsv",
        JSON_DOWNLOAD_SUMMARY_RESPONSE="MT_Historical_Valletta.json",
        CSV_PARQUET_URLS_RESPONSE="MT_Historical_Valletta.csv",
    )
    if name in text_response:
        res = resources.files(__package__).joinpath(text_response[name])
        assert res.is_file(), f"{res} is missing"
        return res.read_text()

    binary_response = dict(
        ZIP_CSV_METADATA_RESPONSE="MT_metadata.csv.zip",
    )
    if name in binary_response:
        res = resources.files(__package__).joinpath(binary_response[name])
        assert res.is_file(), f"{res} is missing"
        return res.read_bytes()

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
