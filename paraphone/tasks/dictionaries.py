import csv
import re
from pathlib import Path
from shutil import copyfile
from typing import Iterable, Tuple, List, Set, Dict, DefaultDict, Optional

import pandas as pd
from pandas._libs.internals import defaultdict
from sortedcontainers import SortedDict
from tqdm import tqdm

from .base import BaseTask
from ..utils import DICTIONARIES_FOLDER, logger, DATA_FOLDER, Phoneme, Syllable, count_lines
from ..workspace import Workspace, WorkspaceCSV


class DictionaryCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, List[Phoneme], Optional[List[Syllable]]]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                phonemes = row["phonetic"].split()
                if row["syllabic"]:
                    syllables = [syllable.split()
                                 for syllable in row["syllabic"].split("-")]
                else:
                    syllables = None
                yield row["word"], phonemes, syllables


FoldingDict = Dict[Tuple[str], Tuple[str]]


class FoldingCSV(WorkspaceCSV):
    folding_dict: FoldingDict
    folding_pho_len: Dict[int, Set[Tuple[str]]]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator=",", header=None)

    def __iter__(self) -> Iterable[Tuple[Tuple[Phoneme], Tuple[Phoneme]]]:
        with self.dict_reader as dict_reader:
            orig_col, ipa_col = dict_reader.fieldnames
            for row in dict_reader:
                yield (tuple(row[orig_col].split(" ")),
                       tuple(row[ipa_col].split(" ")))

    def to_dict(self) -> Dict[Tuple[str], Tuple[str]]:
        return {orig_phones: ipa_phones for orig_phones, ipa_phones in self}

    def load(self):
        # The naming for this dict is folding_phones -> folded_phones
        self.folding_dict = self.to_dict()
        # this dict maps the length of each folding candidate -> set of folding candidates
        folding_pho_len: DefaultDict[int, Set[Tuple[str]]] = defaultdict(set)
        for pho in self.folding_dict:
            folding_pho_len[len(pho)].add(pho)
        self.folding_pho_len = SortedDict(folding_pho_len.items())

    def fold(self, phones: List[str]) -> List[str]:
        # TODO comment this: it's a vile piece of code
        # TODO: move this function to utils.py, as it's going to be needed elsewhere
        word_phones = list(phones)  # making a copy of the list
        output_phones = []
        while word_phones:
            found_fold = False
            # checking longer folding candidates first
            for pho_len, foldings_list in self.folding_pho_len.items():
                candidates = tuple(word_phones[:pho_len])
                for folding_phone in foldings_list:
                    if candidates == folding_phone:
                        folded_phones = self.folding_dict[folding_phone]
                        output_phones += folded_phones
                        word_phones = word_phones[pho_len:]
                        found_fold = True
                        break
                if found_fold:  # breaking out of second loop
                    break
            else:
                raise ValueError(f"Couldn't fold phones in {phones}, stuck "
                                 f"at {word_phones}")
        return output_phones


class DictionarySetupTask(BaseTask):
    creates = ["dictionaries/"]
    DICT_SUBDIR: Path
    FOLDING_PATH: Path
    folding_csv: FoldingCSV

    def load_folding(self):
        self.folding_csv = FoldingCSV(DATA_FOLDER / self.FOLDING_PATH)
        self.folding_csv.load()

    def fold_dictionary(self, workspace: Workspace):
        unfolded_dict_path = workspace.dictionaries / self.DICT_SUBDIR / Path("dict.csv")
        folded_dict_path = workspace.dictionaries / self.DICT_SUBDIR / Path("dict_folded.csv")
        unfolded_csv = DictionaryCSV(unfolded_dict_path)
        folded_csv = DictionaryCSV(folded_dict_path)
        logger.info(f"Folding {unfolded_dict_path} with {folded_dict_path} with folding {self.FOLDING_PATH}")
        with folded_csv.dict_writer as csv_writer:
            csv_writer.writeheader()
            for word, phonetic, syllabic in tqdm(unfolded_csv, total=unfolded_csv.lines_count):
                try:
                    folded = self.folding_csv.fold(phonetic)
                except ValueError as err:
                    logger.error(f"Error in phonemize/fold for word {word}: {err}")
                    continue
                csv_writer.writerow({
                    "word": word,
                    "phonetic": " ".join(folded),
                    "syllabic": syllabic
                })

    def setup_dict_csv(self, workspace: Workspace):
        dict_dir = workspace.dictionaries / self.DICT_SUBDIR
        dict_dir.mkdir(parents=True, exist_ok=True)
        return DictionaryCSV(dict_dir / Path("dict.csv"))

    def copy_folding(self, workspace: Workspace):
        dict_dir = workspace.dictionaries / self.DICT_SUBDIR
        copyfile(DATA_FOLDER / self.FOLDING_PATH, dict_dir / Path("folding.csv"))


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
        self.FOLDING_PATH = Path(f"foldings/{lang}/phonemizer.csv")
        self.copy_folding(workspace)


class LexiqueSetupTask(DictionarySetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/lexique/",
        "dictionaries/vowels.txt",
        "dictionaries/lexique/dict.csv",
        "dictionaries/lexique/folding.csv",
        "dictionaries/lexique/onsets.txt",  # used by wordseg as a trainset
    ]

    DICT_SUBDIR: Path = Path("lexique/")
    FOLDING_PATH = Path("foldings/fr/lexique.csv")

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
            break  # WARNING: added to use just beginning of word as onset

    def fold_onsets(self, onsets: Set[str], folding_csv: FoldingCSV):
        for onset in onsets:
            yield " ".join(folding_csv.fold(list(onset)))

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
        self.load_folding()
        onsets = set(self.fold_onsets(onsets, self.folding_csv))

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
        logger.info(f"Copying vowels to {vowels_path}")
        copyfile(DATA_FOLDER / Path("vowels_fr.txt"), vowels_path)

        # folding dictionary
        self.fold_dictionary(workspace)


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
    FOLDING_PATH = Path("foldings/en/celex.csv")

    ALL_PHONEMES = {'3:', '@', '@U', 'A:', 'A~:', 'D', 'E', 'I', 'I@', 'N', 'O:',
                    'OI', 'O~:', 'O', 'S', 'T', 'U', 'U@', '&', '&~:', '&~',
                    'V', 'Z', 'aI', 'aU', 'b', 'd', 'dZ', 'eI', 'f', 'g', 'h', 'i:',
                    'j', 'k', 'l', 'm', 'n', 'p', 'r', 'r*',
                    's', 't', 'tS', 'u:', 'v', 'w', 'x', 'z'}

    CONSONANTS = {
        'N', 'Q', 'S', 'T',
        'Z', 'b', 'd', 'dZ', 'f', 'g', 'h',
        'j', 'k', 'l', 'm', 'n', 'p', 'r', 'r*',
        's', 't', 'tS', 'v', 'w', 'x', 'z',
    }

    def __init__(self, celex_path: Optional[Path]):
        super().__init__()
        self.celex_path = celex_path
        folding_pho_len: DefaultDict[int, Set[str]] = defaultdict(set)
        for pho in self.ALL_PHONEMES:
            folding_pho_len[len(pho)].add(pho)
        self.folding_pho_len = SortedDict(folding_pho_len.items())

    def find_onset(self, phonemes: List[str]) -> List[str]:
        onset = []
        for pho in phonemes:
            if pho in self.CONSONANTS:
                onset.append(pho)
            else:
                break
        return onset if onset else None

    def fold_onsets(self, onsets: Set[Tuple[str]], folding_csv: FoldingCSV):
        for onset in onsets:
            yield " ".join(folding_csv.fold(list(onset)))

    def parse_phonemes(self, phonemic_form: str) -> List[str]:
        input_phonemes = str(phonemic_form)
        parsed_phonemes = []
        while phonemic_form:
            found_phone = False
            # checking longer folding candidates first
            for pho_len, phonemes in reversed(self.folding_pho_len.items()):
                candidates = phonemic_form[:pho_len]
                for pho in phonemes:
                    if candidates == pho:
                        parsed_phonemes.append(pho)
                        phonemic_form = phonemic_form[pho_len:]
                        found_phone = True
                        break
                if found_phone:  # breaking out of second loop
                    break
            else:
                raise ValueError(f"Couldn't parse phones in {input_phonemes}, stuck "
                                 f"at {phonemic_form}")
        return parsed_phonemes

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        onsets: Set[Tuple[str]] = set()

        clx_phon_re = re.compile(r"\[(.+?)\]")
        entries_count = count_lines(self.celex_path)
        logger.info(f"Loading celex words from {self.celex_path}")
        with open(self.celex_path) as celex_file, dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for line in tqdm(celex_file, desc="CELEX", total=entries_count):
                row = line.strip().split("\\")
                celex_syllabic_form = row[8].replace(",", "")
                re_match = clx_phon_re.findall(celex_syllabic_form)
                phonemes = self.parse_phonemes("".join(list(re_match)))
                onset = self.find_onset(phonemes)
                if onset is not None:
                    onsets.add(tuple(self.find_onset(phonemes)))
                dict_writer.writerow({
                    "word": row[1],
                    "phonetic": " ".join(phonemes),
                    "syllabic": None})

        # copying and loading foldings
        self.copy_folding(workspace, DATA_FOLDER / Path(f"foldings/en/celex.csv"))
        foldings_csv = FoldingCSV(workspace.dictionaries / Path("celex/folding.csv"))
        foldings_csv.load()
        onsets = set(self.fold_onsets(onsets, foldings_csv))

        onsets_path = workspace.dictionaries / Path("celex/onsets.txt")
        logger.info(f"Writing onsets file to {onsets_path}")
        with open(onsets_path, "w") as onsets_file:
            for onset in onsets:
                onsets_file.write(" ".join(onset) + "\n")

        # copying vowels
        vowels_path = workspace.dictionaries / Path("vowels.txt")
        logger.info(f"Copying vowels to {vowels_path}")
        copyfile(DATA_FOLDER / Path("vowels_en.txt"), vowels_path)

        # folding the whole dict
        self.load_folding()
        self.fold_dictionary(workspace)


class CMUSetupTask(DictionarySetupTask):
    dict_file = ""
    secondary_word_re = re.compile(r"\([0-9]+\)$")

    def run(self, workspace: Workspace):
        dict_csv = self.setup_dict_csv(workspace)
        cmu_filepath = DICTIONARIES_FOLDER / Path(self.dict_file)
        logger.info(f"Loading CMU dict from file {cmu_filepath}.")
        lines_count = count_lines(cmu_filepath)
        with open(cmu_filepath) as dict_txt, dict_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for line in tqdm(dict_txt, desc="CMU", total=lines_count):
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

        self.copy_folding(workspace)
        self.load_folding()
        self.fold_dictionary(workspace)


class CMUFRSetupTask(CMUSetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/cmu_fr/",
        "dictionaries/cmu_fr/dict.csv",
    ]
    dict_file = "cmu_fr.txt"
    DICT_SUBDIR = Path("cmu_fr")
    FOLDING_PATH = Path("foldings/fr/cmu.csv")


class CMUENSetupTask(CMUSetupTask):
    creates = DictionarySetupTask.creates + [
        "dictionaries/cmu_en/",
        "dictionaries/cmu_en/dict.csv",
    ]
    dict_file = "cmu_en.txt"
    DICT_SUBDIR = Path("cmu_en")
    FOLDING_PATH = Path("foldings/en/cmu.csv")

# NOTE: for foldings, store default foldings in the package's "data" folder,
# but allow imports of custom foldings
