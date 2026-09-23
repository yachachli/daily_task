try:
    from dotenv import load_dotenv

    _ = load_dotenv()
except ImportError:
    pass

import os


class Env:
    DB_NAME = os.environ["DB_NAME"]
    DB_USER = os.environ["DB_USER"]
    DB_PASS = os.environ["DB_PASS"]
    DB_HOST = os.environ["DB_HOST"]

    API_KEY = os.environ["API_KEY"]

    NBA_ANALYSIS_API_URL = os.environ["NBA_ANALYSIS_API_URL"]
    NBA_ALT_ANALYSIS_API_URL = os.environ["NBA_ALT_ANALYSIS_API_URL"]
    NFL_ANALYSIS_API_URL = os.environ["NFL_ANALYSIS_API_URL"]
    MLB_ANALYSIS_API_URL = os.environ["MLB_ANALYSIS_API_URL"]
    WNBA_ANALYSIS_API_URL = os.environ["WNBA_ANALYSIS_API_URL"]
    # Optional on purpose. MLB is the one league with two POU endpoints -- the
    # hitter model cannot analyse a pitcher -- and the pitcher one lives beside
    # the hitter one, so it is derived when unset. Declaring it required would
    # mean editing every workflow's env block at once, and a required field
    # that one workflow misses is precisely how NBA_ALT_ANALYSIS_API_URL killed
    # Delete Old Bets on every run for three months.
    MLB_PITCHER_ANALYSIS_API_URL = os.environ.get(
        "MLB_PITCHER_ANALYSIS_API_URL",
        MLB_ANALYSIS_API_URL.replace("v2_mlb_pou", "v2_mlb_pitcher_pou"),
    )
    TRANSLATE_ES_URL = os.environ.get("TRANSLATE_ES_URL", "")
