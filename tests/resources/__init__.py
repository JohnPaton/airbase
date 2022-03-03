import json
from importlib import resources

CSV_LINKS_RESPONSE_TEXT = resources.read_text(
    __package__, "csv_links_response.txt"
)

CSV_RESPONSE = resources.read_text(__package__, "csv_response.csv")

METADATA_RESPONSE = resources.read_text(__package__, "metadata.tsv")
