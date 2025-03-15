import json
import logging
import logging.config
import os
import pathlib


def setup_logging():
    config_path = pathlib.Path("./daily_bets/logging-config.json")
    with open(config_path) as f:
        config = json.load(f)

    logging.config.dictConfig(config)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    os.makedirs("./logs", exist_ok=True)


logger = logging.getLogger("app")
