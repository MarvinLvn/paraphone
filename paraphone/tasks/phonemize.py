from .base import BaseTask
from ..workspace import Workspace


class BasePhonemizer:

    def fold(self):
        pass

    def phonemize(self):
        pass


class PhonemizerWrapper(BasePhonemizer):
    pass


class CMUFrenchPhonemizer(BasePhonemizer):
    pass


class CMUEnglishPhonemizer(BasePhonemizer):
    pass


class LexiquePhonemizer(BasePhonemizer):
    pass


class CelexPhonemizer(BasePhonemizer):
    pass


class PhonemizeTask(BaseTask):
    requires = [
    ]

    creates = [
        "phonemized/all.csv"
    ]


class PhonemizeFrenchTask(BaseTask):
    requires = [
        "dictionnaries/lexique/dict.csv",
        "dictionnaries/lexique/folding.csv",
        "dictionnaries/cmu_fr/dict.csv",
        "dictionnaries/cmu_fr/folding.csv",
        "dictionnaries/phonemizer/folding.csv",
    ]

    def run(self, workspace: Workspace):
        pass


class PhonemizeEnglishTask(BaseTask):
    requires = [
        "dictionnaries/celex/dict.csv",
        "dictionnaries/celx/folding.csv",
        "dictionnaries/cmu_en/dict.csv",
        "dictionnaries/cmu_en/folding.csv",
        "dictionnaries/phonemizer/folding.csv",
    ]
    def run(self, workspace: Workspace):
        pass
