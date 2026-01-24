import logging
import os

BASE_PATH = os.path.dirname(__file__)
URL = "https://login.sbisec.co.jp/login/entry"

def setup_logger():
    logger = logging.getLogger("sbirpa")
    if logger.hasHandlers():
        return logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_handler.setLevel(logging.INFO)
    file_handler = logging.FileHandler(os.path.join(log_dir, "rpa.log"))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger
