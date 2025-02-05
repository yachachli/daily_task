import json
import logging
import logging.config
import pathlib

def setup_logging():
    config_path = pathlib.Path("./daily_nfl_bets/logging-config.json")
    with open(config_path) as f:
        config = json.load(f)
    logging.config.dictConfig(config)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger("app")
