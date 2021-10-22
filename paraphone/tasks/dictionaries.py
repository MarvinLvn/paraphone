from .base import BaseTask
from ..workspace import Workspace


class DictionarySetupTask(BaseTask):
    creates = ["dictionaries/"]


class LexiqueSetupTask(BaseTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/lexique/",
        "dictionaries/lexique/dict.csv",
        "dictionaries/lexique/folding.csv",
        "dictionaries/lexique/onsets.txt",  # used by wordseg as a trainset
    ]


class INSEESetupTask(BaseTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/insee/",
        "dictionaries/insee/dict.csv",
    ]


class CelexSetupTask(BaseTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/celex/",
        "dictionaries/celex/dict.csv",
        "dictionaries/celex/folding.csv",
        "dictionaries/celex/onsets.txt",  # used by wordseg as a trainset
    ]


class CMUFRSetupTask(BaseTask):

    creates = DictionarySetupTask.creates + [
        "dictionaries/cmu_fr/",
        "dictionaries/cmu_fr/dict.csv",
    ]

    def run(self, workspace: Workspace):
        pass


class CMUENSetupTask(BaseTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/cmu_en/",
        "dictionaries/cmu_en/dict.csv",
    ]

    def run(self, workspace: Workspace):
        pass

# NOTE: for foldings, store default foldings in the package's "data" folder,
# but allow imports of custom foldings
