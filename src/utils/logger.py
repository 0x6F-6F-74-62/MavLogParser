import logging
import os
from datetime import datetime


def setup_logger(
    name: str, log_level: int = logging.INFO, log_to_file: bool = False, log_dir: str = "logs"
) -> logging.Logger:
    """
    Set up and configure a logger with optional file and console handlers.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_to_file:
            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            log_file_path = os.path.join(log_dir, f"{name}_{timestamp}.log")
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger
