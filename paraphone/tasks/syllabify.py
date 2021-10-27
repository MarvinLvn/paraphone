from pathlib import Path

from yaml import safe_dump

from .base import BaseTask
from ..workspace import Workspace


class WordsegTrainTask(BaseTask):
    requires = [
        "phonemized/all.csv"
    ]

    def run(self, workspace: Workspace):
        pass

class SillabifyTask(BaseTask):
    pass

