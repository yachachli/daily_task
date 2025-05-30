try:
    from dotenv import load_dotenv

    load_dotenv()
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
    NFL_ANALYSIS_API_URL = os.environ["NFL_ANALYSIS_API_URL"]
    MLB_ANALYSIS_API_URL = os.environ["MLB_ANALYSIS_API_URL"]
