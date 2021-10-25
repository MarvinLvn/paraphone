import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from .base import BaseTask
from ..utils import DICTIONARIES_FOLDER
from ..workspace import Workspace, WorkspaceCSV


class DictionaryCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)


class DictionarySetupTask(BaseTask):
    creates = ["dictionaries/"]
    dict_subdir: Path = Path("dictionaries/")


class LexiqueSetupTask(BaseTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/lexique/",
        "dictionaries/lexique/dict.csv",
        "dictionaries/lexique/folding.csv",
        "dictionaries/lexique/onsets.txt",  # used by wordseg as a trainset
    ]

    DICT_SUBDIR: Path = Path("dictionaries/lexique/")
    VOWELS = {"a", "i", "y", "u", "o", "O", "E", "°", "2", "9", "5", "1",
              "@", "§", "3", "j", "8", "w"}
    CONSONANTS = {"p", "b", "t", "d", "k", "g", "f", "v", "s", "z", "S",
                  "Z", "m", "n", "N", "l", "R", "x", "G"}
    ONSET_RE = re.compile(f'[{"".join(CONSONANTS)}]+')

    def find_onsets(self, syllabic_form: str) -> Iterable[str]:
        syllables = syllabic_form.split("-")
        for syllable in syllables:
            onset_match = self.ONSET_RE.match(syllable)
            if onset_match is not None:
                yield onset_match[0]

    def run(self, workspace: Workspace):
        dict_dir = workspace.root_path / self.DICT_SUBDIR
        dict_dir.mkdir(parents=True, exist_ok=True)

        dict_csv = DictionaryCSV(dict_dir / Path("dict.cst"))
        # reading the csv and extracting only the columns of interest
        lexique_df = pd.read_csv(DICTIONARIES_FOLDER / Path("lexique_383.tsv"),
                                 sep="\t")
        lexique_df = lexique_df[["ortho", "phon", "syll"]]
        with dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for _, row in lexique_df.iterrows():
                word = row["ortho"]  # word
                phonetic = " ".join(list([row["phon"]]))  # dEd@ -> d E d @
                syllabic = " ".join(list([row["syll"]]))  # dE-d@ -> d E - d @
                dict_writer.writerow(
                    {"word": word, "phonetic": phonetic, "syllabic": syllabic}
                )


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

    def __init__(self, celex_folder: Path):
        self.celex_folder = celex_folder


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
