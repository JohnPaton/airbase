import sys

if sys.version_info >= (3, 11):
    from importlib import resources
else:
    import importlib_resources as resources


CSV_LINKS_RESPONSE_TEXT: str = (
    resources.files(__package__).joinpath("csv_links_response.txt").read_text()
)

CSV_RESPONSE: str = (
    resources.files(__package__).joinpath("csv_response.csv").read_text()
)

METADATA_RESPONSE: str = (
    resources.files(__package__).joinpath("metadata.tsv").read_text()
)
