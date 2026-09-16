import logging
import sys
from dotenv import load_dotenv

load_dotenv()
_setup_complete = False


def setup_logging(level: int = logging.INFO):
    global _setup_complete
    if _setup_complete:
        return
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    local_logging()
    _setup_complete = True


def local_logging():
    print("===== Local environment detected. Setting up console logger. =====")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

def get_logger(name):
    setup_logging()
    return logging.getLogger(name)
