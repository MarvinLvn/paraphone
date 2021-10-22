import logging
from datetime import datetime
from logging import StreamHandler, Formatter
from pathlib import Path

logger = logging.getLogger("paraphone")
logger.setLevel(logging.DEBUG)
stream_formatter = Formatter("[%(levelname)s] %(message)s")
stream_handler = StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)


def setup_file_handler(cmd_name: str, folder: Path):
    file_formatter = Formatter("[%(levelname)s]:%(name)s|%(message)s")
    file_handler = logging.FileHandler(folder / Path(f"{cmd_name}_{datetime.now().isoformat()}"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)


def pairwise(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(a, a)
