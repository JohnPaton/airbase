import json
from importlib import resources

with resources.path(__package__, "summary.json") as path:
    SUMMARY = json.loads(path.read_text())

with resources.path(__package__, "csv_links_response.txt") as path:
    CSV_LINKS_RESPONSE_TEXT = path.read_text()

with resources.path(__package__, "csv_response.csv") as path:
    CSV_RESPONSE = path.read_text()

with resources.path(__package__, "metadata.tsv") as path:
    METADATA_RESPONSE = path.read_text()
