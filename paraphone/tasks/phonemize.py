from pathlib import Path
from typing import List, Set, Iterable, Tuple, Dict, DefaultDict

import tqdm
from pandas._libs.internals import defaultdict
from phonemizer import phonemize
from phonemizer.separator import Separator
from sortedcontainers import SortedDict

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

    def to_dict(self) -> Dict[Tuple[str], Tuple[str]]:
        return {orig_phones: ipa_phones for orig_phones, ipa_phones in self}


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
        # The naming for this dict is folding_phones -> folded_phones
        self.folding_dict = folding_csv.to_dict()
        # this dict maps the length of each folding candidate -> set of folding candidates
        folding_pho_len: DefaultDict[int, Set[Tuple[str]]] = defaultdict(set)
        for pho in self.folding_dict:
            folding_pho_len[len(pho)].add(pho)
        self.folding_pho_len = SortedDict(folding_pho_len.items())

        words_dict = DictionaryCSV(dictionary_folder / Path("dict.csv"))
        self.words_dict = {word: phonemes for word, phonemes, _ in words_dict}

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
                raise ValueError(f"Coudnl't fold phones in {phones}, stuck "
                                 f"at {word_phones}")

        return output_phones

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
        "datasets/tokenized/all.csv"
    ]

    creates = [
        "phonemized/all.csv"
    ]

    def load_phonemizers(self, workspace: Workspace) -> List[BasePhonemizer]:
        raise NotImplemented()

    def run(self, workspace: Workspace):
        workspace.phonemized.mkdir(parents=True, exist_ok=True)
        tokenized_words_csv = TokenizedTextCSV(workspace.tokenized / Path("all.csv"))
        phonemizers = self.load_phonemizers(workspace)
        phonemized_words_csv = PhonemizedWordsCSV(workspace.phonemized / Path("all.csv"))

        # computing length of tokenized words file
        pbar = tqdm.tqdm(total=count_lines(tokenized_words_csv.file_path))

        logger.info("Phonemizing all words in tokenized dataset...")
        with phonemized_words_csv.dict_writer as dict_writer:
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
                        logger.error(f"Error in phonemize/fold : {err}")
                        return
                    else:
                        dict_writer.writerow({
                            "word": word,
                            "phones": folded_phones
                        })
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
