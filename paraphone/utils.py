import logging
from datetime import datetime
from itertools import tee, zip_longest
from logging import StreamHandler, Formatter
from pathlib import Path
from typing import List

Phoneme = str
Syllable = List[str]

DATA_FOLDER = Path(__file__).parent / Path("data")
DICTIONARIES_FOLDER = DATA_FOLDER / Path("dictionaries/")

logger = logging.getLogger("paraphone")
logger.setLevel(logging.DEBUG)
stream_formatter = Formatter("[%(levelname)s] %(message)s")
stream_handler = StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)


def setup_file_handler(cmd_name: str, folder: Path):
    folder.mkdir(parents=True, exist_ok=True)
    file_formatter = Formatter("[%(levelname)s]:%(name)s|%(message)s")
    file_handler = logging.FileHandler(folder / Path(f"{cmd_name}_{datetime.now().isoformat()}"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)


def pairwise(iterable):
    """s -> (s0, s1), (s2, s3), (s4, s5), ..."""
    a = iter(iterable)
    return zip(a, a)


def consecutive_pairs(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def chunkify(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    for group in zip_longest(*args, fillvalue=fillvalue):
        yield [e for e in group if e is not None]


def _count_generator(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def count_lines(filepath: Path) -> int:
    """A fast line counter for text files"""
    with open(filepath, 'rb') as fp:
        c_generator = _count_generator(fp.read)
        # count each \n
        return sum(buffer.count(b'\n') for buffer in c_generator)


def null_logger():
    """Configures and returns a logger sending messages to nowhere
    This is used as default logger for some functions.
    Returns
    -------
    logging.Logger
        Logging instance ignoring all the messages.
    """
    log = logging.getLogger("nulllogger")
    log.addHandler(logging.NullHandler())
    return log


def parse_syllabic(syllabic_form: str) -> List[Syllable]:
    return [syllable.split(" ") for syllable in syllabic_form.split("-")]


def fmt_syllabic(syllables: List[Syllable]) -> str:
    return "-".join(" ".join(pho) for pho in syllables)
