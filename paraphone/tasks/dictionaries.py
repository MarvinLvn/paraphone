import csv
import re
from pathlib import Path
from typing import Iterable, Tuple, List, Set

import pandas as pd
from tqdm import tqdm

from .base import BaseTask
from ..utils import DICTIONARIES_FOLDER
from ..workspace import Workspace, WorkspaceCSV

Phoneme = str
Syllable = List[str]


class DictionaryCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, List[Phoneme], List[Syllable]]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                phonemes = row["phonetic"].split()
                syllables = [syllable.split()
                             for syllable in row["syllabic"].split("-")]
                yield row["word"], phonemes, syllables


class DictionarySetupTask(BaseTask):
    creates = ["dictionaries/"]
    DICT_SUBDIR: Path = Path("dictionaries/")

    def setup_dict_csv(self, workspace: Workspace):
        dict_dir = workspace.root_path / self.DICT_SUBDIR
        dict_dir.mkdir(parents=True, exist_ok=True)
        return DictionaryCSV(dict_dir / Path("dict.cst"))


class LexiqueSetupTask(DictionarySetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/lexique/",
        "dictionaries/lexique/dict.csv",
        "dictionaries/lexique/folding.csv",
        "dictionaries/lexique/onsets.txt",  # used by wordseg as a trainset
    ]

    DICT_SUBDIR: Path = Path("dictionaries/lexique/")
    VOWELS = {"a", "i", "y", "u", "o", "O", "E", "°", "2", "9", "5", "1",
              "@", "§", "3"}
    CONSONANTS = {"p", "b", "t", "d", "k", "g", "f", "v", "s", "z", "S",
                  "Z", "m", "n", "N", "l", "R", "x", "G", "j", "8", "w"}
    ONSET_RE = re.compile(f'[{"".join(CONSONANTS)}]+')

    def find_onsets(self, syllabic_form: str) -> Iterable[str]:
        syllables = syllabic_form.split("-")
        for syllable in syllables:
            onset_match = self.ONSET_RE.match(syllable)
            if onset_match is not None:
                yield onset_match[0]

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        # reading the csv and extracting only the columns of interest
        lexique_df = pd.read_csv(DICTIONARIES_FOLDER / Path("lexique_383.tsv"),
                                 sep="\t")
        lexique_df = lexique_df[["ortho", "phon", "syll"]]

        onsets: Set[str] = set()
        with dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for _, row in tqdm(lexique_df.iterrows(), total=len(lexique_df)):
                word = row["ortho"]  # word
                phonetic = " ".join(list([row["phon"]]))  # dEd@ -> d E d @
                syllabic = " ".join(list([row["syll"]]))  # dE-d@ -> d E - d @
                dict_writer.writerow(
                    {"word": word, "phonetic": phonetic, "syllabic": syllabic}
                )
                onsets.update(set(self.find_onsets(row["syll"])))

        onsets_filepath = workspace.root_path / self.DICT_SUBDIR / Path("onsets.txt")
        with open(onsets_filepath, "w") as onsets_file:
            onsets_file.writelines(onsets)


class INSEESetupTask(DictionarySetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/insee/",
        "dictionaries/insee/dict.csv",
    ]
    DICT_SUBDIR = Path("insee")

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        all_names = set()

        last_names_csv = DICTIONARIES_FOLDER / Path("insee_lastnames.csv")
        first_names_csv = DICTIONARIES_FOLDER / Path("insee_firstnames.csv")
        with dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()

            with open(last_names_csv) as insee_csv:
                dict_reader = csv.DictReader(insee_csv,
                                             fieldnames=["patronyme", "count"],
                                             delimiter=",")
                for row in dict_reader:
                    word = row["patronyme"].lower()
                    if word in all_names:
                        continue

                    all_names.add(word)
                    dict_writer.writerow({
                        "word": word, "phonetic": None, "syllabic": None
                    })

            with open(first_names_csv) as insee_csv:
                dict_reader = csv.DictReader(insee_csv,
                                             fieldnames=["prenom", "sum"],
                                             delimiter=",")
                for row in dict_reader:
                    word = row["prenom"].lower()
                    if word in all_names:
                        continue

                    all_names.add(word)
                    dict_writer.writerow({
                        "word": word, "phonetic": None, "syllabic": None
                    })


class CelexSetupTask(DictionarySetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/celex/",
        "dictionaries/celex/dict.csv",
        "dictionaries/celex/folding.csv",
        "dictionaries/celex/onsets.txt",  # used by wordseg as a trainset
    ]

    def __init__(self, celex_folder: Path):
        super().__init__()
        self.celex_folder = celex_folder

    def run(self, workspace: Workspace):
        pass  # TODO


class CMUSetupTask(DictionarySetupTask):
    dict_file = ""
    secondary_word_re = re.compile(r"\([0-9]+\)$")

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        with open(DICTIONARIES_FOLDER / Path(self.dict_file)) as dict_txt:
            with dict_csv.dict_writer as dict_writer:
                dict_writer.writeheader()
                for line in dict_txt:
                    line = line.strip("-")
                    word, *phonemes = line.split(" ")
                    # if word is of the type "read(2)" (and thus a secondary
                    # pronunciation), ignore
                    if self.secondary_word_re.search(word) is not None:
                        continue
                    dict_writer.writerow({
                        "word": word,
                        "phonetic:": " ".join(phonemes),
                        "syllabic": None  # no syllabic representation for CMU
                    })


class CMUFRSetupTask(CMUSetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/cmu_fr/",
        "dictionaries/cmu_fr/dict.csv",
    ]
    dict_file = "cmu_fr.txt"
    DICT_SUBDIR = Path("cmu_fr")


class CMUENSetupTask(CMUSetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/cmu_en/",
        "dictionaries/cmu_en/dict.csv",
    ]
    dict_file = "cmu_en.txt"
    DICT_SUBDIR = Path("cmu_en")

# NOTE: for foldings, store default foldings in the package's "data" folder,
# but allow imports of custom foldings
