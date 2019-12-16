"""Global variables for URL templating"""
import datetime

E1A_SUMMARY_URL = "http://discomap.eea.europa.eu/map/fme/E1a/summaryE1a.js"

LINK_LIST_URL_TEMPLATE = (
    "http://fme.discomap.eea.europa.eu/fmedatastreaming/"
    "AirQualityDownload/AQData_Extract.fmw"
    "?CountryCode={country}"
    "&CityName="
    "&Pollutant={shortpl}"
    "&Year_from={year_from}"
    "&Year_to={year_to}"
    "&Station="
    "&Samplingpoint="
    "&Source={source}"
    "&Output=TEXT"
    "&UpdateDate={update_date}"
)

METADATA_URL = (
    "http://discomap.eea.europa.eu/map/fme/metadata/PanEuropean_metadata.csv"
)

CURRENT_YEAR = str(datetime.datetime.today().year)

DATE_FMT = "%Y-%m-%d %T"

ALL_SOURCES = ["E1a", "E2a", "All"]
