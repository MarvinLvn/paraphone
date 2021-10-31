from .base import BaseTask
from ..workspace import Workspace, WorkspaceCSV
from wordseg.syllabification import Syllabifier as SBF
from wordseg.separator import Separator
from wordseg.utils import get_logger


class SyllabifiedWordsCSV(WorkspaceCSV):
    pass

class SillabifyTask(BaseTask):
    requires = [
        "phonemized/all.csv",
    ]

    creates = [
        "phonemized/syllabic.csv"
    ]

    def run(self, workspace: Workspace):
        pass


class SillabifyFrenchTask(BaseTask):
    requires = SillabifyTask.requires + [
        "dictionaries/lexique/onsets.txt"
    ]


class SillabifyEnglishTask(BaseTask):
    requires = SillabifyTask.requires + [
        "dictionaries/celex/onsets.txt"
    ]
