import random
import re
from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Tuple, List, Iterable, Set, Optional

import Levenshtein
from tqdm import tqdm

from ..base import BaseTask, CorporaTaskMixin
from ..tokenize import TokenizedWordsCSV
from ..wuggy_gen import FakeWordsCandidatesCSV
from ...utils import logger
from ...workspace import Workspace, WorkspaceCSV


@dataclass
class WordPair:
    word: str
    word_pho: str
    fake_word_pho: str


class CandidatesPairCSV(WorkspaceCSV):
    header = ["word", "word_pho", "fake_word_pho"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, str, str]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield row["word"], row["word_pho"], row["fake_word_pho"]


class FilteringTaskMixin(BaseTask):
    step_re = re.compile("step_([0-9]+).*")
    step_name: str

    @classmethod
    def next_step_filename(cls, previous_step_id: int) -> Path:
        return Path(f"step_{previous_step_id + 1}_{cls.step_name}.csv")

    def previous_step_filepath(self, workspace: Workspace) -> Tuple[Path, int]:
        """Finds the last output from the previous step in the filtering
        sub-pipeline"""
        filtering_steps_folder = workspace.candidates_filtering / Path("steps")
        found_steps_files: List[Tuple[Path, int]] = list()
        for filepath in filtering_steps_folder.iterdir():
            re_match = self.step_re.match(filepath.name)
            if re_match is not None:
                found_steps_files.append((filepath, int(re_match[1])))
        return max(found_steps_files, key=lambda x: x[1])

    def keep_pair(self, word_pair: WordPair) -> bool:
        raise NotImplemented()

    def filter(self, workspace: Workspace):
        """Filters out candidates based on the `keep_pair` function's output"""

        previous_step_path, previous_step_id = self.previous_step_filepath(workspace)
        input_csv = CandidatesPairCSV(previous_step_path)
        output_csv = CandidatesPairCSV(previous_step_path.parent
                                       / self.next_step_filename(previous_step_id))

        logger.info(f"Filtering pairs from {input_csv.file_path} into {output_csv.file_path}")
        pairs_count = input_csv.lines_count
        kept_count = 0
        with output_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, word_pho, fake_word_pho in tqdm(input_csv, total=pairs_count):
                word_pair = WordPair(word, word_pho, fake_word_pho)
                if self.keep_pair(word_pair):
                    kept_count += 1
                    dict_writer.writerow({
                        "word": word, "word_pho": word_pho, "fake_word_pho": fake_word_pho
                    })


class CorpusFinalFilteringTask(FilteringTaskMixin, CorporaTaskMixin, BaseTask):
    """Specific version of the `FilteringTaskMixin` made for the final
    step of the pipeline, and that is corpus-specific."""

    requires = [
        "corpora/tokenized/*.csv"
    ]

    creates = [
        "corpora/wuggy_pairs/*.csv"
    ]

    def __init__(self, for_corpus: Optional[int] = None):
        super().__init__()
        self.for_corpus = for_corpus

    @classmethod
    def get_tokenized_corpus(cls, workspace: Workspace, corpus_id: int) -> TokenizedWordsCSV:
        csv_path =  workspace.corpora / Path(f"tokenized/corpus_{corpus_id}.csv")
        return TokenizedWordsCSV(csv_path)

    def filter(self, workspace: Workspace, corpus_id: int):  # noqa
        """Filters out candidates based on the `keep_pair` function's output,
        and only for the """

        previous_step_path, previous_step_id = self.previous_step_filepath(workspace)
        input_csv = CandidatesPairCSV(previous_step_path)
        output_corpora_folder = workspace.corpora / Path("wuggy_pairs")
        output_corpora_folder.mkdir(parents=True, exist_ok=True)
        output_csv = CandidatesPairCSV(output_corpora_folder / Path(f"corpus_{corpus_id}.csv"))

        # retrieving the tokenized word list of corpus to eliminate all words
        # not contained in that corpus
        tokenized_corpus_csv = self.get_tokenized_corpus(workspace, corpus_id)
        corpus_words_pho: Set[str] = {word for word, _ in tokenized_corpus_csv}

        logger.info(f"Filtering pairs (for corpus {corpus_id}), "
                    f"from {input_csv.file_path} into {output_csv.file_path}")
        pairs_count = input_csv.lines_count
        kept_count = 0
        with output_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, word_pho, fake_word_pho in tqdm(input_csv, total=pairs_count):
                if word not in corpus_words_pho:
                    continue
                word_pair = WordPair(word, word_pho, fake_word_pho)
                if self.keep_pair(word_pair):
                    kept_count += 1
                    dict_writer.writerow({
                        "word": word, "word_pho": word_pho, "fake_word_pho": fake_word_pho
                    })

    def run_for_corpus(self, workspace: Workspace, corpus_id: int):
        raise NotImplemented()

    def run(self, workspace: Workspace):
        corpora = self.find_corpora(workspace.corpora / Path("tokenized/"))
        if self.for_corpus is not None:
            assert self.for_corpus in {corpus_id for corpus_id, _ in corpora}
            corpora = [(self.for_corpus, None)]

        for corpus_id, _, in corpora:
            logger.info(f"Running {self.__class__.__name__} for corpus {corpus_id}.")
            self.run_for_corpus(workspace, corpus_id)


class InitFilteringTask(FilteringTaskMixin):
    requires = [
        "wuggy/candidates.csv",
    ]

    creates = [
        "candidates_filtering/steps/",
        "candidates_filtering/steps/step_1_init.csv"
    ]

    def run(self, workspace: Workspace):
        steps_folder = workspace.candidates_filtering / Path("steps")
        steps_folder.mkdir(parents=True, exist_ok=True)

        wuggy_candidates_csv = FakeWordsCandidatesCSV(workspace.wuggy / Path("candidates.csv"))
        step_init_csv = CandidatesPairCSV(steps_folder / Path("step_1_init.csv"))

        logger.info(f"Initializing filtering pipeline with {wuggy_candidates_csv.file_path}")
        with step_init_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, word_pho, _, fake_word_pho, _ in tqdm(wuggy_candidates_csv,
                                                            total=wuggy_candidates_csv.lines_count):
                dict_writer.writerow({
                    "word": word,
                    "word_pho": " ".join(word_pho),
                    "fake_word_pho": " ".join(fake_word_pho)
                })


class RandomFilterTask(FilteringTaskMixin):
    """Keeps a random split of the candidates determined by ratio."""
    step_name = "random"

    def __init__(self, ratio: float):
        super().__init__()
        assert 0 < ratio < 1.0
        self.ratio = ratio

    def keep_pair(self, word_pair: WordPair) -> bool:
        return random.random() < self.ratio

    def run(self, workspace: Workspace):
        random.seed(4577)
        self.filter(workspace)


class RandomPairFilterTask(FilteringTaskMixin):
    """Keeps only one of the word/nonword pairs, at random"""
    step_name = "random-pairs"

    def __init__(self):
        super().__init__()
        self._chosen_pairs: Set[Tuple[str, str]] = set()

    def keep_pair(self, word_pair: WordPair) -> bool:
        return (word_pair.word_pho, word_pair.fake_word_pho) in self._chosen_pairs

    def run(self, workspace: Workspace):
        random.seed(4577)
        previous_step_csv_path, previous_step_id = self.previous_step_filepath(workspace)
        previous_step_csv = CandidatesPairCSV(previous_step_csv_path)
        word_nonword = defaultdict(list)  # word -> list(nonwords)
        for _, word_pho, fake_word_pho in tqdm(previous_step_csv):
            word_nonword[word_pho].append(fake_word_pho)

        for word, fake_words in tqdm(word_nonword.items()):
            chosen_fake_word = random.choice(fake_words)
            self._chosen_pairs.add((word, chosen_fake_word))
        self.filter(workspace)


class EqualsFilterTask(FilteringTaskMixin):
    """Filters out equal phonetic pairs"""
    step_name = "equals"

    def keep_pair(self, word_pair: WordPair) -> bool:
        return word_pair.word_pho != word_pair.fake_word_pho

    def run(self, workspace: Workspace):
        self.filter(workspace)


class LevenshteinFilterTask(FilteringTaskMixin):
    """Keeps pairs whose edit distance is lower than a threshold"""
    step_name = "random"

    def __init__(self, max_distance: int):
        super().__init__()
        assert max_distance > 0
        self.max_distance = max_distance

    def pho_to_str(self, pho_a: List[str], pho_b: List[str]) -> Tuple[str, str]:
        """Remaps phoneme lists to ASCII strings for levenshtein edit distance"""
        pho_set = set(chain.from_iterable([pho_a, pho_b]))
        pho_map = {pho: chr(i) for i, pho in enumerate(pho_set)}
        return "".join(pho_map[pho] for pho in pho_a), "".join(pho_map[pho] for pho in pho_b)

    def keep_pair(self, word_pair: WordPair) -> bool:
        word_pho, fake_word_pho = self.pho_to_str(word_pair.word_pho.split(" "),
                                                  word_pair.fake_word_pho.split(" "))
        return Levenshtein.distance(word_pho, fake_word_pho) <= self.max_distance

    def run(self, workspace: Workspace):
        self.filter(workspace)
