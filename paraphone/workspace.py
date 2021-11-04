import csv
from contextlib import contextmanager
from csv import DictReader, DictWriter
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Tuple, List, Iterable, Optional, Dict, ContextManager

import pandas as pd
import yaml

from .utils import count_lines


class WorkspaceCSV:

    def __init__(self, file_path: Path,
                 separator: str = "\t",
                 header: Optional[List[str]] = None):
        self.file_path = file_path
        self.separator = separator
        self.header = header

    @property
    @contextmanager
    def dict_reader(self) -> ContextManager[DictReader]:
        with open(self.file_path, "r") as csv_file:
            yield csv.DictReader(csv_file,
                                 delimiter=self.separator)

    @property
    @contextmanager
    def dict_writer(self) -> ContextManager[DictWriter]:
        with open(self.file_path, "w") as csv_file:
            yield csv.DictWriter(csv_file,
                                 fieldnames=self.header,
                                 delimiter=self.separator)

    @cached_property
    def lines_count(self):
        return count_lines(self.file_path)

    def to_pandas(self):
        return pd.read_csv(str(self.file_path),
                           sep=self.separator,
                           header="infer" if self.header is not None else None)

    def write(self, data: List[Dict]):
        with self.dict_writer as dict_writer:
            dict_writer.writeheader()
            for row_data in data:
                dict_writer.writerow(row_data)

    def __iter__(self) -> Iterable[Tuple]:
        pass


@dataclass
class Workspace:
    root_path: Path

    @property
    def config(self) -> Dict:
        with open(self.root_path / Path("config.yml")) as cfg_file:
            return yaml.safe_load(cfg_file)

    @property
    def datasets(self) -> Path:
        return self.root_path / Path("datasets/")

    @property
    def datasets_index(self) -> Path:
        return self.datasets / Path("index.csv")

    @property
    def tokenized(self) -> Path:
        return self.datasets / Path("tokenized/")

    @property
    def dictionaries(self) -> Path:
        return self.root_path / Path("dictionaries")

    @property
    def phonemized(self) -> Path:
        return self.root_path / Path("phonemized/")

    @property
    def corpora(self) -> Path:
        return self.root_path / Path("corpora/")

    @property
    def wuggy(self) -> Path:
        return self.root_path / Path("wuggy/")

    @property
    def synth(self) -> Path:
        return self.root_path / Path("synth/")

    @property
    def logs(self) -> Path:
        return self.root_path / Path("logs/")