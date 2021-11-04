from pathlib import Path
from typing import Tuple, List

from wordseg.separator import Separator
from wordseg.syllabification import Syllabifier

from .base import BaseTask
from .phonemize import PhonemizedWordsCSV
from ..utils import logger
from ..workspace import Workspace, WorkspaceCSV


class SyllabifiedWordsCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)


class SillabifyTask(BaseTask):
    requires = [
        "phonemized/all.csv",
    ]

    creates = [
        "phonemized/syllabic.csv"
    ]
    phonetic_dict: str

    def load_phonetic_config(self, workspace) -> Tuple[List[str], List[str]]:
        phonetic_dict_path = workspace.dictionaries / Path(self.phonetic_dict)
        with open(phonetic_dict_path / Path("onsets.txt")) as onsets_file:
            onsets = onsets_file.read().split("\n")

        with open(phonetic_dict_path / Path("vowels.txt")) as vowels_file:
            vowels = vowels_file.read().split("\n")

        return onsets, vowels

    def run(self, workspace: Workspace):
        logger.info(f"Loading onsets and vowels from dictionary {self.phonetic_dict}")
        onsets, vowels = self.load_phonetic_config(workspace)

        syllabifier = Syllabifier(onsets, vowels,
                                  Separator(phone=" ", syllable="/", word=";"),
                                  log=logger)
        logger.info("Syllabifying phonemized words")
        phonemized_words_path = workspace.phonemized / Path("all.csv")
        phonemized_words_csv = PhonemizedWordsCSV(phonemized_words_path)

        graphemic_forms, phonetic_forms = phonemized_words_csv
        syllabic_forms = syllabifier.syllabify(phonetic_forms)

        syllabified_csv = SyllabifiedWordsCSV(workspace.phonemized / Path("syllabic.csv"))
        with syllabified_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, phonetic, syllabic in zip(graphemic_forms,
                                                phonetic_forms,
                                                syllabic_forms):
                # replacing syllables delimiter with "-" (more reader-friendly) IMHO
                # if you disagree, fite me
                syllabic = syllabic.replace("/", "-")
                dict_writer.writerow({
                    "word": word,
                    "phonetic": phonetic,
                    "syllabic": syllabic
                })


class SillabifyFrenchTask(BaseTask):
    requires = SillabifyTask.requires + [
        "dictionaries/lexique/onsets.txt"
        "dictionaries/lexique/vowels.txt"
    ]
    phonetic_dict = "lexique"


class SillabifyEnglishTask(BaseTask):
    requires = SillabifyTask.requires + [
        "dictionaries/celex/onsets.txt"
        "dictionaries/celex/vowels.txt"
    ]
    phonetic_dict = "celex"
