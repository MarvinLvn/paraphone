from pathlib import Path
from typing import Tuple, List, Iterable, Dict

from tqdm import tqdm

from .base import BaseTask
from .dictionaries import Phoneme, Syllable
from .phonemize import PhonemizedWordsCSV
from ..syllable_seg.separator import Separator
from ..syllable_seg.syllabification import Syllabifier, NoVowelError, NoOnsetError, UnknownSymbolError
from ..utils import logger
from ..workspace import Workspace, WorkspaceCSV


class SyllabifiedWordsCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, List[Phoneme], List[Syllable]]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield (row["word"],
                       row["phonetic"].split(" "),
                       [syllable.split(" ") for syllable in row["syllabic"].split("-")])

    def to_dict(self) -> Dict[str, Tuple[List[Phoneme], List[Syllable]]]:
        return {word: (pho, syll) for word, pho, syll in self}


class SyllabifyTask(BaseTask):
    requires = [
        "phonemized/all.csv",
    ]

    creates = [
        "phonemized/syllabic.csv"
    ]
    phonetic_dict: str
    stats_subpath = Path("syllabify.yaml")

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

        # instantiating a syllabifier
        syllabifier = Syllabifier(onsets, vowels,
                                  Separator(phone=" ", syllable="/", word=";"),
                                  log=logger)
        logger.info("Syllabifying phonemized words")
        phonemized_words_path = workspace.phonemized / Path("all.csv")
        phonemized_words_csv = PhonemizedWordsCSV(phonemized_words_path)

        # building the stats dict for failed syllabification
        self._stats = {err.__class__: 0 for err in [NoVowelError,
                                                    NoOnsetError,
                                                    UnknownSymbolError]}

        graphemic_forms, phonetic_forms = zip(*phonemized_words_csv)
        phonetic_forms = [" ".join(form) for form in phonetic_forms]
        logger.info("Syllabifying the phonemic forms")
        syllabic_forms = []
        for pho_form in tqdm(phonetic_forms):
            try:
                syll_form = syllabifier.syllabify(pho_form, strip=True)
            except RuntimeError as err:
                logger.debug(f"Couldn't syllabify {pho_form}: {err}")
                self._stats[err.__class__] += 1
                syllabic_forms.append(None)
            else:
                syllabic_forms.append(syll_form)

        syllabified_csv = SyllabifiedWordsCSV(workspace.phonemized / Path("syllabic.csv"))
        syllabified_count = 0
        with syllabified_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, phonetic, syllabic in zip(graphemic_forms,
                                                phonetic_forms,
                                                syllabic_forms):
                if syllabic is None:
                    continue
                # replacing syllables delimiter with "-" (more reader-friendly) IMHO
                # if you disagree, fite me
                syllabic = syllabic.replace("/", "-")
                dict_writer.writerow({
                    "word": word,
                    "phonetic": phonetic,
                    "syllabic": syllabic
                })
                syllabified_count += 1

        logger.info(f"Syllabifyed {syllabified_count} words")
        logger.info(f"Dropped {sum(self._stats.values())} words because of syllabification errors")
        for err_type, count in self._stats.items():
            logger.info(f"- {count} dropped because of {err_type}")


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
