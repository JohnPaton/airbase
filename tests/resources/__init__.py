import json
from importlib import resources

SUMMARY = json.loads(resources.read_text(__package__, "summary.json"))

CSV_LINKS_RESPONSE_TEXT = resources.read_text(
    __package__, "csv_links_response.txt"
)

CSV_RESPONSE = resources.read_text(__package__, "csv_response.csv")

METADATA_RESPONSE = resources.read_text(__package__, "metadata.tsv")
