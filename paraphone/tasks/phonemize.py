import logging
from pathlib import Path
from typing import List, Set, Iterable, Tuple

import tqdm
from phonemizer import phonemize
from phonemizer.separator import Separator

from .base import BaseTask
from .dictionaries import DictionaryCSV, FoldingCSV
from .tokenize import TokenizedTextCSV
from ..utils import count_lines, logger, Phoneme
from ..workspace import Workspace, WorkspaceCSV


class PhonemizedWordsCSV(WorkspaceCSV):
    header = ["word", "phones"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, List[Phoneme]]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield row["word"], row["phones"].split(" ")


class BasePhonemizer:
    folder_name = ""
    requires = []

    def __init__(self, workspace: Workspace):
        dictionary_folder = workspace.dictionaries / Path(self.folder_name)
        self.folding_csv = FoldingCSV(dictionary_folder / Path("folding.csv"))
        self.folding_csv.load()

        words_dict = DictionaryCSV(dictionary_folder / Path("dict.csv"))
        self.words_dict = {word: phonemes for word, phonemes, _ in words_dict}

    def fold(self, phones: List[str]) -> List[str]:
        return self.folding_csv.fold(phones)

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

    def phonemize(self, word: str) -> List[str]:
        phonemized = phonemize(word,
                               language=self.lang,
                               separator=self.separator,
                               strip=True,
                               language_switch="remove-utterance")
        if not phonemized:
            raise KeyError(word)

        return phonemized.strip().split(" ")


class CMUFrenchPhonemizer(BasePhonemizer):
    folder_name = "cmu_fr"
    requires = [
        "dictionaries/cmu_fr/dict.csv",
        "dictionaries/cmu_fr/folding.csv",
    ]


class CMUEnglishPhonemizer(BasePhonemizer):
    folder_name = "cmu_en"
    requires = [
        "dictionaries/cmu_en/dict.csv",
        "dictionaries/cmu_en/folding.csv",
    ]


class LexiquePhonemizer(BasePhonemizer):
    folder_name = "lexique"
    requires = [
        "dictionaries/lexique/dict.csv",
        "dictionaries/lexique/folding.csv",
    ]


class CelexPhonemizer(BasePhonemizer):
    folder_name = "celex"
    requires = [
        "dictionaries/celex/dict.csv",
        "dictionaries/celex/folding.csv",
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
        tokenized_words_csv = TokenizedTextCSV(workspace.tokenized / Path("all.csv"))

        # loading phonemizer instances
        phonemizers = self.load_phonemizers(workspace)
        phonemized_words_csv = PhonemizedWordsCSV(workspace.phonemized / Path("all.csv"))

        # building stats dict for phonemization
        self._stats = {phnmzr.__class__.__name__: 0 for phnmzr in phonemizers}

        # set of all phonemized forms
        phonemized_words: Set[str] = set()

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
                        folded_phones = phonemizer.fold(phones)
                    except KeyError:
                        continue
                    except ValueError as err:
                        logger.error(f"Error in phonemize/fold for word {word}: {err}")
                        return
                    else:
                        # if current word's phonetic form is already present,
                        # ignore word (else, add it to the current set of phonemized words)
                        phonetic_form = "".join(folded_phones)
                        if phonetic_form in phonemized_words:
                            break
                        else:
                            phonemized_words.add(phonetic_form)

                        # logging the phonemization in the stats
                        self._stats[phonemizer.__class__.__name__] += 1

                        dict_writer.writerow({
                            "word": word,
                            "phones": " ".join(folded_phones)
                        })
                        break
                else:
                    logger.warning(f"Couldn't phonemize word {word}")


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
        return [CMUFrenchPhonemizer(workspace),
                CelexPhonemizer(workspace),
                PhonemizerWrapper(workspace, lang="en")]
