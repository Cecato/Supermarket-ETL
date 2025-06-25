import logging
import sys
import os

def get_logger(name="supermarket-etl", level=logging.INFO, log_dir="log"):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(f"{log_dir}/{name}.log")
        file_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger
