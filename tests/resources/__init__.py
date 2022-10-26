import sys
from importlib import resources


def _read_text(resource: str) -> str:
    """compatibility wrapper for Python 3.11 importlib.resources"""
    if sys.version_info >= (3, 11):
        source = resources.files(__package__) / resource
        with resources.as_file(source) as path:
            return path.read_text()

    return resources.read_text(__package__, resource)


CSV_LINKS_RESPONSE_TEXT = _read_text("csv_links_response.txt")

CSV_RESPONSE = _read_text("csv_response.csv")

METADATA_RESPONSE = _read_text("metadata.tsv")
