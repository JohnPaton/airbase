from airbase.summary import DB


def __getattr__(name: str):
    if name == "COUNTRY_CODES":
        return set(DB.countries())
    if name == "POLLUTANT_NOTATIONS":
        return set(DB.pollutants())
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
