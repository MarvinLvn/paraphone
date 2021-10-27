from pathlib import Path
from typing import List, Set, Iterable, Tuple

import tqdm
from phonemizer import phonemize
from phonemizer.separator import Separator

from .base import BaseTask
from .dictionaries import Phoneme, DictionaryCSV
from .tokenize import TokenizedTextCSV
from ..utils import count_lines, logger
from ..workspace import Workspace, WorkspaceCSV


class FoldingCSV(WorkspaceCSV):
    def __init__(self, file_path: Path):
        super().__init__(file_path, separator=",", header=None)

    def __iter__(self) -> Iterable[Tuple[Tuple[Phoneme], Tuple[Phoneme]]]:
        with self.dict_reader as dict_reader:
            orig_col, ipa_col = dict_reader.fieldnames
            for row in dict_reader:
                yield (tuple(row[orig_col].split(" ")),
                       tuple(row[ipa_col].split(" ")))

    def to_dict(self):
        return {orig_phon: ipa_phon for orig_phon, ipa_phon in self}


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
        dictionary_folder = (workspace.root_path
                             / Path(f"dictionary/{self.folder_name}"))
        folding_csv = FoldingCSV(dictionary_folder / Path("folding.csv"))
        self.folding_dict = folding_csv.to_dict()
        words_dict = DictionaryCSV(dictionary_folder / Path("dict.csv"))
        self.words_dict = {word: phonemes for word, phonemes, _ in words_dict}

    def fold(self, phones: List[str]) -> List[str]:
        pass  # TODO

    def phonemize(self, word: str) -> List[str]:
        return self.words_dict[word]


class PhonemizerWrapper(BasePhonemizer):

    def __init__(self, workspace: Workspace,  # noqa
                 lang: str = "fr"):
        dictionary_folder = (workspace.root_path
                             / Path(f"dictionary/{self.folder_name}"))
        folding_csv = FoldingCSV(dictionary_folder / Path("folding.csv"))
        self.folding_dict = folding_csv.to_dict()

        self.lang = "fr-fr" if lang == "fr" else "en-us"
        self.separator = Separator(phone=" ", word=",")

    def phonemize(self, word: str) -> List[str]:
        return phonemize("test",
                         language=self.lang,
                         separator=self.separator).split(" ")


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
        # TODO: add dependency on tokenized text
    ]

    creates = [
        "phonemized/all.csv"
    ]

    def load_phonemizers(self, workspace: Workspace) -> List[BasePhonemizer]:
        raise NotImplemented()

    def run(self, workspace: Workspace):
        phonemized_dir = workspace.root_path / Path("phonemized")
        phonemized_dir.mkdir(parents=True, exist_ok=True)

        phonemizers = self.load_phonemizers(workspace)
        tokenized_texts_folder = workspace.root_path / Path("datasets/tokenized/")
        phonemized_words: Set[str] = set()
        phonemized_words_csv = PhonemizedWordsCSV(phonemized_dir / Path("all.csv"))

        # evaluating the total number of words to phonemize to have a reliable
        # progress bar representation
        logger.info("Estimating total word counts")
        word_count = sum(count_lines(csv_filepath) - 1
                         for csv_filepath in tokenized_texts_folder.iterdir())
        pbar = tqdm.tqdm(total=word_count)

        logger.info("Phonemizing all words in tokenized dataset...")
        with phonemized_words_csv.dict_writer as dict_writer:
            for csv_filepath in tokenized_texts_folder.iterdir():
                tokenized_csv = TokenizedTextCSV(csv_filepath)
                pbar.set_description(f"For file {csv_filepath.name}")
                for word, _ in tokenized_csv:
                    pbar.update()
                    # skipping already phonemized words
                    if word in phonemized_words:
                        continue

                    # trying to phonemize with each phonemizer, in their order
                    for phonemizer in phonemizers:
                        try:
                            phones = phonemizer.phonemize(word)
                        except KeyError:
                            continue
                        else:
                            folded_phones = phonemizer.fold(phones)
                            dict_writer.writerow({
                                "word": word,
                                "phones": folded_phones
                            })
                            phonemized_words.add(word)
                            break
                    else:
                        raise RuntimeError(f"Couldn't phonemize word {word}")


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
