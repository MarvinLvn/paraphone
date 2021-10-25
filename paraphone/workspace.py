import csv
from contextlib import contextmanager
from csv import DictReader, DictWriter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Tuple, List, Iterable, Optional, Type, Dict, ContextManager

import pandas as pd


@dataclass
class WorkspaceElement:
    name: Path
    parent: Path

    @property
    def path(self) -> Path:
        return self.parent / self.name

    def exists(self, recursive: bool = False):
        assert self.path.exists(), f"Missing workspace file: {self.path}"


@dataclass
class WorkspaceFolder(WorkspaceElement):
    children: List[WorkspaceElement] = field(default_factory=list)

    @property
    def iter_children(self) -> Iterable[WorkspaceElement]:
        yield from self.children

    def exists(self, recursive: bool = False):
        super().exists()
        if recursive:
            for child in self.children:
                child.exists(recursive=recursive)


@dataclass
class CSVFilesFolder(WorkspaceFolder):
    files_pattern: Optional[str] = None


@dataclass
class WorkSpaceFile(WorkspaceElement):
    pass


@dataclass
class WorkspaceFile:
    pass


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
                                 fieldnames=self.header,
                                 delimiter=self.separator)

    @property
    @contextmanager
    def dict_writer(self) -> ContextManager[DictWriter]:
        with open(self.file_path, "r") as csv_file:
            yield csv.DictWriter(csv_file,
                                 fieldnames=self.header,
                                 delimiter=self.separator)

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

    def find_files(self, path_template: str,
                   class_wrapper: Optional[Type[WorkspaceFile]] = None,
                   class_kwargs: Optional[Dict] = None):
        pass  # TODO
