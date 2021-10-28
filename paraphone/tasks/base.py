import abc
import logging
from pathlib import Path
from typing import List, Dict

from paraphone.paraphone.utils import logger
from paraphone.paraphone.workspace import Workspace


class BaseTask(metaclass=abc.ABCMeta):
    requires: List[str]
    creates: List[str]

    def __init__(self):
        self._output_context: Dict[str, str] = {}
        self._requirements_context: Dict[str, str] = {}

    def check_requirements(self):
        """Checks the presence of required files"""
        pass

    def check_output(self, workspace: Workspace):
        """Checks the presence of created files"""
        for path_template in self.creates:

            if "*" in path_template:
                found_files = list(workspace.root_path.glob(path_template))
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

    # @abstractmethod
    def run(self, workspace: Workspace):
        pass
