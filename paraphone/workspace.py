from dataclasses import dataclass
from pathlib import Path
from typing import Tuple, List

import pandas as pd
from traitlets import List


@dataclass
class WorkspaceElement:
    name: Path
    parent: Path


@dataclass
class WorkspaceFolder(WorkspaceElement):
    children: List[WorkspaceElement]


@dataclass
class WorkSpaceFile(WorkspaceElement):
    pass


@dataclass
class WorkspaceCSV(WorkSpaceFile):
    separator: str = "\t"
    has_headers: bool = True

    def to_pandas(self):
        return pd.read_csv(str(self.path),
                           sep=self.separator,
                           header="infer" if self.has_headers else None)

    def __iter__(self) -> Tuple:
        pass


class Workspace:

    def __init__(self, root_path: Path):
        self.root = root_path
