import collections
import re
from pathlib import Path
from typing import List, Set, Iterable, Tuple, Counter, Dict

from tqdm import tqdm

from .base import BaseTask
from .dictionaries import DictionaryCSV
from .imports import DatasetIndexCSV, FileID
from ..utils import count_lines
from ..workspace import Workspace, WorkspaceCSV


class TokenizedTextCSV(WorkspaceCSV):
    header = ["word", "count"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)
        self._words: Counter[str] = collections.Counter()

    def add_word(self, word: str):
        self._words[word] += 1

    def write_entries(self):
        self.write([{"word": word, "count": count}
                    for word, count in self._words.items()])

    def __iter__(self) -> Iterable[Tuple[str, int]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield row["word"], int(row["count"])

    def to_dict(self) -> Dict[str, int]:
        return {word: count for word, count in self}


class TokenizeTask(BaseTask):
    requires = [
        "datasets/raw/",
        "datasets/index.csv",
    ]

    creates = [
        "datasets/tokenized/*.csv"
    ]

    non_letters_re: re.Pattern
    _dictionaries: List[Set[str]]
    dict_names: List[str]

    def load_dictionaries(self, workspace: Workspace) -> List[Set[str]]:
        dicts: List[Set[str]] = []
        for dict_name in self.dict_names:
            dict_csv = DictionaryCSV(workspace.root_path /
                                     Path(f"dictionaries/{dict_name}/dict.csv"))
            dicts.append(
                {word for word, _, _ in dict_csv}
            )
        return dicts

    def tokenize_file(self, text_id: FileID, file_path: Path, workspace: Workspace):
        tokenization_csv = TokenizedTextCSV(
            workspace.root_path / Path("datasets/tokenized/") / Path(f"{text_id}.csv")
        )
        with open(file_path) as txt_file:
            for line in txt_file:
                # cleaning up line of text (replacing all non-letters by spaces)
                cleaned_line = self.non_letters_re.sub(" ", line)
                # splitting line by whitespace:
                candidates = cleaned_line.split()

                # for each word candidate:
                # - test if it's in any of the dictionnaries, in their order
                # - if it's in none of them and contains a "-", add the
                # candidate's subwords as future candidates
                while candidates:
                    word_candidate = candidates.pop().lower()
                    for word_dict in self._dictionaries:
                        if word_candidate in word_dict:
                            tokenization_csv.add_word(word_candidate)
                            break
                    else:
                        if "-" in word_candidate:
                            candidates += word_candidate.split("-")

        tokenization_csv.write_entries()

    def run(self, workspace: Workspace):
        # creating "tokenized" directory
        (workspace.root_path / Path("datasets/tokenized/")).mkdir(parents=True,
                                                                  exist_ok=True)
        dataset_index = DatasetIndexCSV(Path("datasets/index.csv"))

        self._dictionaries = self.load_dictionaries(workspace)

        # for each text file in dataset, tokenize
        for text_id, text_path in tqdm(list(dataset_index)):
            self.tokenize_file(text_id, text_path, workspace)


class TokenizeFrenchTask(TokenizeTask):
    requires = TokenizeTask.requires + [
        "dictionnaries/cmu_fr/dict.csv",
        "dictionnaries/lexique/dict.csv",
        "dictionnaries/insee/dict.csv",
    ]
    dict_names = ["cmu_fr", "lexique", "insee"]
    non_letters_re = re.compile(r"[!'(),./0123456789:;?\[\]_«°»]")


class TokenizeEnglishTask(TokenizeTask):
    requires = TokenizeTask.requires + [
        "dictionnaries/cmu_en/dict.csv",
        "dictionnaries/celex/dict.csv",
    ]
    dict_names = ["cmu_en", "celex"]
    non_letters_re = re.compile(r"[^a-zA-Z'-]+")
