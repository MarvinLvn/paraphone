import abc
import re
from pathlib import Path
from typing import List, Dict, Optional, Iterable, Tuple

import yaml

from ..utils import logger
from ..workspace import Workspace


class BaseTask(metaclass=abc.ABCMeta):
    requires: List[str] = []
    creates: List[str] = []
    stats_subpath: Optional[Path] = None

    def __init__(self):
        self._output_context: Dict[str, str] = {}
        self._input_context: Dict[str, str] = {}
        self._requirements_context: Dict[str, str] = {}
        self.stats: Dict = dict()

    def check_requirements(self, workspace: Workspace):
        """Checks the presence of required files"""
        for path_template in self.requires:
            if "*" in path_template:
                found_files = list(workspace.root_path.glob(path_template.format(**self._input_context)))
                if found_files:
                    for file_path in found_files:
                        logger.debug(f"Found required file {file_path}")
                else:
                    raise FileNotFoundError(f"Found no files for pattern {path_template}")
            else:
                file_path = (workspace.root_path
                             / Path(path_template.format(**self._output_context)))
                if file_path.exists():
                    logger.debug(f"Found required file {file_path}")
                else:
                    raise FileNotFoundError(f"Requirement file {file_path} not found.")

    def check_output(self, workspace: Workspace):
        """Checks the presence of created files"""
        for path_template in self.creates:

            if "*" in path_template:
                found_files = list(workspace.root_path.glob(path_template.format(**self._output_context)))
                if found_files:
                    for file_path in found_files:
                        logger.debug(f"Created file {file_path}")
                else:
                    logger.warning(f"Found no created files for pattern {path_template}")
            else:
                file_path = (workspace.root_path
                             / Path(path_template.format(**self._output_context)))
                if file_path.exists():
                    logger.debug(f"Created file {file_path}")
                else:
                    logger.warning(f"File {file_path} was not created.")

    def write_stats(self, workspace: Workspace):
        if self.stats_subpath is not None:
            with open(workspace.stats / self.stats_subpath) as stats_file:
                yaml.safe_dump(self.stats, stats_file)

    # @abstractmethod
    def run(self, workspace: Workspace):
        pass


class CorporaTaskMixin:
    corpus_name_re = re.compile(r"corpus_([0-9]+)")

    def iter_corpora(self, folder: Path) -> Iterable[Tuple[int, Path]]:
        for filepath in folder.iterdir():
            re_match = self.corpus_name_re.match(filepath.name)
            if re_match is not None:
                yield int(re_match[1], filepath)
