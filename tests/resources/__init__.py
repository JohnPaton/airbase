import sys

if sys.version_info >= (3, 11):
    from importlib import resources
else:
    import importlib_resources as resources

METADATA_RESPONSE: str
CSV_PARQUET_URLS_RESPONSE: str


def __getattr__(name: str):
    resource = dict(
        METADATA_RESPONSE="metadata.tsv",
        CSV_PARQUET_URLS_RESPONSE="MT_Historical_Valletta.csv",
    )
    if name not in resource:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    return resources.files(__package__).joinpath(resource[name]).read_text()
