from .base import BaseTask
from ..workspace import Workspace


class TokenizeTask(BaseTask):
    requires = [
        "datasets/raw/",
        "datasets/index.csv",
        ""
    ]

    creates = [
        "datasets/tokenized/{file_id}.csv"
    ]

    def run(self, workspace: Workspace):
        pass  # TODO


class TokenizeFrenchTask(TokenizeTask):
    requires = TokenizeTask.requires + [
        "dictionnaries/cmu_fr/dict.csv",
        "dictionnaries/lexique/dict.csv",
        "dictionnaries/insee/dict.csv",
    ]

class TokenizeEnglishTask(TokenizeTask):
    requires = TokenizeTask.requires + [
        "dictionnaries/cmu_en/dict.csv",
    ]
