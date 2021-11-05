from pathlib import Path
from typing import Tuple, List

from ..syllable_seg.separator import Separator
from ..syllable_seg.syllabification import Syllabifier

from .base import BaseTask
from .phonemize import PhonemizedWordsCSV
from ..utils import logger
from ..workspace import Workspace, WorkspaceCSV


class SyllabifiedWordsCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)


class SyllabifyTask(BaseTask):
    requires = [
        "phonemized/all.csv",
    ]

    creates = [
        "phonemized/syllabic.csv"
    ]
    phonetic_dict: str

    def load_phonetic_config(self, workspace: Workspace) -> Tuple[List[str], List[str]]:
        phonetic_dict_path = workspace.dictionaries / Path(self.phonetic_dict)
        with open(phonetic_dict_path / Path("onsets.txt")) as onsets_file:
            onsets = onsets_file.read().split("\n")

        with open(workspace.dictionaries / Path("vowels.txt")) as vowels_file:
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

        graphemic_forms, phonetic_forms = zip(*phonemized_words_csv)
        phonetic_forms = [" ".join(form) for form in phonetic_forms]
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


class SyllabifyFrenchTask(SyllabifyTask):
    requires = SyllabifyTask.requires + [
        "dictionaries/lexique/onsets.txt",
        "dictionaries/vowels.txt"
    ]
    phonetic_dict = "lexique"


class SyllabifyEnglishTask(SyllabifyTask):
    requires = SyllabifyTask.requires + [
        "dictionaries/celex/onsets.txt",
        "dictionaries/vowels.txt"
    ]
    phonetic_dict = "celex"
