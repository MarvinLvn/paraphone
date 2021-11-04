import collections
import logging
import re
from pathlib import Path
from typing import List, Set, Iterable, Tuple, Counter, Dict

from tqdm import tqdm

from .base import BaseTask
from .dictionaries import DictionaryCSV
from .imports import DatasetIndexCSV, FileID
from ..utils import count_lines, logger
from ..workspace import Workspace, WorkspaceCSV


class TokenizedTextCSV(WorkspaceCSV):
    header = ["word", "count"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)
        self.words: Counter[str] = collections.Counter()

    def add_word(self, word: str):
        self.words[word] += 1

    def write_entries(self):
        self.write([{"word": word, "count": count}
                    for word, count in self.words.items()])

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
        "datasets/tokenized/all.csv",
        "datasets/tokenized/per_text/*.csv"
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

    def tokenize_file(self, text_id: FileID,
                      file_path: Path,
                      all_tokenized_csv: TokenizedTextCSV,
                      workspace: Workspace):
        tokenization_csv = TokenizedTextCSV(
            workspace.tokenized / Path(f"per_text/{text_id}.csv")
        )
        with open(file_path) as raw_text_file:
            for line in raw_text_file:
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
                            all_tokenized_csv.add_word(word_candidate)
                            break
                    else:
                        if "-" in word_candidate:
                            candidates += word_candidate.split("-")

        logger.debug(f"Tokenized {len(tokenization_csv.words)} unique words")
        logger.debug(f"Tokenized {sum(tokenization_csv.words.values())} words total")
        tokenization_csv.write_entries()

    def run(self, workspace: Workspace):
        # creating "tokenized" directory
        workspace.tokenized.mkdir(parents=True, exist_ok=True)
        (workspace.tokenized / Path(f"per_text/")).mkdir(parents=True, exist_ok=True)

        all_tokenized_words = TokenizedTextCSV(workspace.tokenized / Path("all.csv"))
        dataset_index = DatasetIndexCSV(workspace.datasets_index)
        self._dictionaries = self.load_dictionaries(workspace)

        # for each text file in dataset, tokenize
        dataset_pbar = tqdm(list(dataset_index))
        for text_id, text_path in dataset_pbar:
            dataset_pbar.set_description(f"For {text_id}")
            try:
                self.tokenize_file(text_id, text_path, all_tokenized_words, workspace)
            except FileNotFoundError:
                logger.warning(f"Couldn't find file {text_id} at path {text_path} in dataset")
        all_tokenized_words.write_entries()


class TokenizeFrenchTask(TokenizeTask):
    requires = TokenizeTask.requires + [
        "dictionaries/cmu_fr/dict.csv",
        "dictionaries/lexique/dict.csv",
        "dictionaries/insee/dict.csv",
    ]
    dict_names = ["cmu_fr", "lexique", "insee"]
    non_letters_re = re.compile(r"[!'(),./0123456789:;?\[\]_«°»]")


class TokenizeEnglishTask(TokenizeTask):
    requires = TokenizeTask.requires + [
        "dictionaries/cmu_en/dict.csv",
        "dictionaries/celex/dict.csv",
    ]
    dict_names = ["cmu_en", "celex"]
    non_letters_re = re.compile(r"[^a-zA-Z'-]+")
