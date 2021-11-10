from pathlib import Path

from ..base import BaseTask
from ...workspace import Workspace


class BaseFilteringTask(BaseTask):

    def previous_step_filepath(self, workspace: Workspace) -> Path:
        pass


class InitFilteringTask(BaseFilteringTask):
    pass