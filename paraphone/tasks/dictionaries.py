import csv
import re
from pathlib import Path
from shutil import copyfile
from typing import Iterable, Tuple, List, Set

import pandas as pd
from tqdm import tqdm

from .base import BaseTask
from .phonemize import FoldingCSV
from ..utils import DICTIONARIES_FOLDER, logger, DATA_FOLDER
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
    DICT_SUBDIR: Path
    FOLDING_PATH: Path

    def setup_dict_csv(self, workspace: Workspace):
        dict_dir = workspace.dictionaries / self.DICT_SUBDIR
        dict_dir.mkdir(parents=True, exist_ok=True)
        return DictionaryCSV(dict_dir / Path("dict.csv"))

    def copy_folding(self, workspace: Workspace, folding_path: Path):
        dict_dir = workspace.dictionaries / self.DICT_SUBDIR
        copyfile(folding_path, dict_dir / Path("folding.csv"))


class PhonemizerSetupTask(DictionarySetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/phonemizer/",
        "dictionaries/phonemizer/folding.csv",
    ]
    DICT_SUBDIR = Path("phonemizer/")

    def run(self, workspace: Workspace):
        dict_dir = workspace.dictionaries / self.DICT_SUBDIR
        dict_dir.mkdir(parents=True, exist_ok=True)
        lang = workspace.config["lang"]
        self.copy_folding(workspace, DATA_FOLDER / Path(f"foldings/{lang}/phonemizer.csv"))


class LexiqueSetupTask(DictionarySetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/lexique/",
        "dictionaries/lexique/dict.csv",
        "dictionaries/lexique/folding.csv",
        "dictionaries/lexique/onsets.txt",  # used by wordseg as a trainset
    ]

    DICT_SUBDIR: Path = Path("lexique/")
    VOWELS = {"a", "i", "y", "u", "o", "O", "E", "°", "2", "9", "5", "1",
              "@", "§", "3"}
    CONSONANTS = {"p", "b", "t", "d", "k", "g", "f", "v", "s", "z", "S",
                  "Z", "m", "n", "N", "l", "R", "x", "G", "j", "8", "w"}
    ONSET_RE = re.compile(f'[{"".join(CONSONANTS)}]+')

    def find_onsets(self, syllabic_form: str) -> Iterable[str]:
        # TODO : fold onsets to normalize them to IPA
        syllables = syllabic_form.split("-")
        for syllable in syllables:
            onset_match = self.ONSET_RE.match(syllable)
            if onset_match is not None:
                yield onset_match[0]  # TODO : investigate onsets

    def fold_onsets(self, onsets: Set[str], folding_csv: FoldingCSV):
        for onset in onsets:
            yield " ".join(folding_csv.fold(onset.split(" ")))

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        # reading the csv and extracting only the columns of interest
        lexique_path = DICTIONARIES_FOLDER / Path("lexique_383.tsv")
        logger.info(f"Loading lexique data from {lexique_path}")
        lexique_df = pd.read_csv(lexique_path, sep="\t")
        lexique_df = lexique_df[["ortho", "phon", "syll"]]

        onsets: Set[str] = set()
        with dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for _, row in tqdm(lexique_df.iterrows(), total=len(lexique_df)):
                word = row["ortho"]  # word
                phonetic = " ".join(list(row["phon"]))  # dEd@ -> d E d @
                syllabic = " ".join(list(row["syll"]))  # dE-d@ -> d E - d @
                dict_writer.writerow(
                    {"word": word, "phonetic": phonetic, "syllabic": syllabic}
                )
                onsets.update(set(self.find_onsets(row["syll"])))

        # copying and loading foldings
        lang = workspace.config["lang"]
        self.copy_folding(workspace, DATA_FOLDER / Path(f"foldings/{lang}/lexique.csv"))
        foldings_csv = FoldingCSV(workspace.dictionaries / Path("lexique/folding.csv"))
        onsets = set(self.fold_onsets(onsets, foldings_csv))

        # removing and adding custom onsets
        with open(DATA_FOLDER / Path(f"onsets/{lang}/added.txt")) as added_onsets_file:
            added_onsets = set(added_onsets_file.read().split("\n"))
            logger.debug(f"Added onsets: {added_onsets}")
            onsets.update(added_onsets)
        with open(DATA_FOLDER / Path(f"onsets/{lang}/removed.txt")) as removed_onsets_file:
            removed_onsets = set(removed_onsets_file.read().split("\n"))
            logger.debug(f"Removed onsets : {removed_onsets}")
            onsets = onsets - removed_onsets

        # saving onsets
        onsets_filepath = dict_csv.file_path.parent / Path("onsets.txt")
        logger.info(f"Saving onsets for lexique to {onsets_filepath}")
        with open(onsets_filepath, "w") as onsets_file:
            for onset in onsets:
                onsets_file.write(onset + "\n")

        # copying vowels
        vowels_path = workspace.dictionaries / Path("vowels.txt")
        logger.info(f"'Copying vowels to {vowels_path}")
        copyfile(DATA_FOLDER / Path("vowels_fr.txt"), vowels_path)


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
        logger.info(f"Loading INSEE first/last names from "
                    f"{last_names_csv} and {first_names_csv}")
        with dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            with open(last_names_csv) as insee_csv:
                dict_reader = csv.DictReader(insee_csv,
                                             fieldnames=["patronyme", "count"],
                                             delimiter=",")
                for row in tqdm(dict_reader, desc="Last Names"):
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
                for row in tqdm(dict_reader, desc="First Names"):
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
    DICT_SUBDIR = Path("celex")

    def __init__(self, celex_folder: Path):
        super().__init__()
        self.celex_folder = celex_folder

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        all_names = set()

        celex_dict_path = self.celex_folder / Path("english/epw/epw.cd")
        with open(celex_dict_path) as celex_file, \
                dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            celex_reader = csv.reader(celex_file, delimiter="\\")
            for row in tqdm(celex_reader):
                word = row[1].lower()

                # TODO: remove prosody information (', _?, #?)
                # TODO: parse phonemes (some have length of two)


class CMUSetupTask(DictionarySetupTask):
    dict_file = ""
    secondary_word_re = re.compile(r"\([0-9]+\)$")

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        cmu_filepath = DICTIONARIES_FOLDER / Path(self.dict_file)
        logger.info(f"Loading CMU dict from file {cmu_filepath}.")
        with open(cmu_filepath) as dict_txt, dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for line in tqdm(dict_txt, desc="CMU"):
                line = line.strip("-")
                word, *phonemes = line.strip().split(" ")
                # if word is of the type "read(2)" (and thus a secondary
                # pronunciation), ignore
                if self.secondary_word_re.search(word) is not None:
                    continue
                dict_writer.writerow({
                    "word": word.lower(),
                    "phonetic": " ".join(phonemes),
                    "syllabic": None  # no syllabic representation for CMU
                })

        lang = workspace.config["lang"]
        self.copy_folding(workspace, DATA_FOLDER / Path(f"foldings/{lang}/cmu.csv"))


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
# TODO: filter words that have the same pronunciation
