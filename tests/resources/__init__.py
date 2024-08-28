import sys

if sys.version_info >= (3, 11):
    from importlib import resources
else:
    import importlib_resources as resources


def __getattr__(name: str):
    resource = dict(
        CSV_LINKS_RESPONSE_TEXT="csv_links_response.txt",
        CSV_RESPONSE="csv_response.csv",
        METADATA_RESPONSE="metadata.tsv",
        JSON_CITY_RESPONSE="city.json",
        JSON_COUNTRY_RESPONSE="country.json",
        JSON_PROPERTY_RESPONSE="property.json",
        CSV_PARQUET_URLS_RESPONSE="MT_Historical_Valletta.csv",
    )
    if name not in resource:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    return resources.files(__package__).joinpath(resource[name]).read_text()
