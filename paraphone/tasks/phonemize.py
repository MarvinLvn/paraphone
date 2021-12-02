import logging
import typing
from collections import Counter
from pathlib import Path
from typing import List, Iterable, Tuple, Dict

import tqdm
from phonemizer.backend import EspeakBackend
from phonemizer.separator import Separator

from .base import BaseTask
from .dictionaries import DictionaryCSV, FoldingCSV
from .tokenize import TokenizedWordsCSV
from ..utils import count_lines, logger, Phoneme, null_logger
from ..workspace import Workspace, WorkspaceCSV


class PhonemizedWordsCSV(WorkspaceCSV):
    header = ["word", "phones"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, List[Phoneme]]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield row["word"], row["phones"].split(" ")

    def to_dict(self) -> Dict[str, List[Phoneme]]:
        return {word: phonemes for word, phonemes in self}

    @property
    def unique_phonemic(self) -> Iterable[List[Phoneme]]:
        phonemic = set()
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                if row["phones"] in phonemic:
                    continue
                phonemic.add(row["phones"])
                yield row["phones"].split(" ")


class BasePhonemizer:
    folder_name = ""
    requires = []

    def __init__(self, workspace: Workspace):
        dictionary_folder = workspace.dictionaries / Path(self.folder_name)
        words_dict = DictionaryCSV(dictionary_folder / Path("dict_folded.csv"))
        self.words_dict = {word: phonemes for word, phonemes, _ in words_dict}

    def phonemize(self, word: str) -> List[str]:
        return self.words_dict[word]


class PhonemizerWrapper(BasePhonemizer):
    folder_name = "phonemizer"

    def __init__(self, workspace: Workspace,  # noqa
                 lang: str = "fr"):
        dictionary_folder = workspace.dictionaries / Path(self.folder_name)
        self.folding_csv = FoldingCSV(dictionary_folder / Path("folding.csv"))
        self.folding_csv.load()
        logging.getLogger("phonemizer").setLevel(logging.CRITICAL)

        self.lang = "fr-fr" if lang == "fr" else "en-us"
        self.separator = Separator(phone=" ", word=None)
        self.backend = EspeakBackend(
            self.lang,
            language_switch="remove-utterance",
            logger=null_logger())

    def fold(self, phones: List[str]) -> List[str]:
        return self.folding_csv.fold(phones)

    def phonemize(self, word: str) -> List[str]:
        phonemized = self.backend.phonemize(text=[word],
                                            separator=self.separator,
                                            strip=True)
        if not phonemized[0]:
            raise KeyError(word)
        phonemes = phonemized[0].strip().split(" ")
        return self.fold(phonemes)


class CMUFrenchPhonemizer(BasePhonemizer):
    folder_name = "cmu_fr"
    requires = [
        "dictionaries/cmu_fr/dict_folded.csv",
    ]


class CMUEnglishPhonemizer(BasePhonemizer):
    folder_name = "cmu_en"
    requires = [
        "dictionaries/cmu_en/dict_folded.csv",
    ]


class LexiquePhonemizer(BasePhonemizer):
    folder_name = "lexique"
    requires = [
        "dictionaries/lexique/dict_folded.csv",
    ]


class CelexPhonemizer(BasePhonemizer):
    folder_name = "celex"
    requires = [
        "dictionaries/celex/dict_folded.csv",
    ]


class PhonemizeTask(BaseTask):
    requires = [
        "datasets/tokenized/all.csv"
    ]

    creates = [
        "phonemized/all.csv"
    ]
    stats_subpath = Path("phonemize.yml")

    def load_phonemizers(self, workspace: Workspace) -> List[BasePhonemizer]:
        raise NotImplemented()

    def run(self, workspace: Workspace):
        workspace.phonemized.mkdir(parents=True, exist_ok=True)
        tokenized_words_csv = TokenizedWordsCSV(workspace.tokenized / Path("all.csv"))

        # loading phonemizer instances
        phonemizers = self.load_phonemizers(workspace)
        phonemized_words_csv = PhonemizedWordsCSV(workspace.phonemized / Path("all.csv"))

        # building stats dict for phonemization
        self.stats = {phnmzr.__class__.__name__: 0 for phnmzr in phonemizers}

        # number of words per unique phonemic form
        phonemized_counter: typing.Counter[str] = Counter()

        # computing length of tokenized words file
        pbar = tqdm.tqdm(total=count_lines(tokenized_words_csv.file_path))

        logger.info("Phonemizing all words in tokenized dataset...")
        with phonemized_words_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, _ in tokenized_words_csv:
                pbar.update()

                # trying to phonemize with each phonemizer, in their order
                for phonemizer in phonemizers:
                    try:
                        phones = phonemizer.phonemize(word)
                    except KeyError:
                        continue
                    except ValueError as err:
                        logger.error(f"Error in phonemize/fold for word {word}: {err}")
                        return
                    else:
                        # if current word's phonetic form is already present,
                        # ignore word (else, add it to the current set of phonemized words)
                        phonemized_counter["".join(phones)] += 1

                        # logging the phonemization in the stats
                        self.stats[phonemizer.__class__.__name__] += 1

                        dict_writer.writerow({
                            "word": word,
                            "phones": " ".join(phones)
                        })
                        break
                else:
                    logger.warning(f"Couldn't phonemize word {word}")

        # storing the number of unique phonetic forms and n-plicates phonetic form
        self.stats["n_plicates_count"] = dict(Counter(phonemized_counter.values()))


class PhonemizeFrenchTask(PhonemizeTask):
    requires = (CMUFrenchPhonemizer.requires
                + LexiquePhonemizer.requires
                + PhonemizerWrapper.requires)

    def load_phonemizers(self, workspace: Workspace) -> List[BasePhonemizer]:
        return [CMUFrenchPhonemizer(workspace),
                LexiquePhonemizer(workspace),
                PhonemizerWrapper(workspace, lang="fr")]


class PhonemizeEnglishTask(PhonemizeTask):
    requires = (CMUEnglishPhonemizer.requires
                + CelexPhonemizer.requires
                + PhonemizerWrapper.requires)

    def load_phonemizers(self, workspace: Workspace) -> List[BasePhonemizer]:
        return [CMUEnglishPhonemizer(workspace),
                CelexPhonemizer(workspace),
                PhonemizerWrapper(workspace, lang="en")]
