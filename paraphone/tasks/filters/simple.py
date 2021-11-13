import random
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Set, Tuple, List

import Levenshtein
from tqdm import tqdm

from paraphone.tasks.filters.base import FilteringTaskMixin, CandidatesPairCSV, WordPair, CorpusFinalFilteringTask
from paraphone.tasks.wuggy_gen import FakeWordsCandidatesCSV
from paraphone.utils import logger
from paraphone.workspace import Workspace


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


class PassThroughFinalFilter(CorpusFinalFilteringTask):
    """Just copies the last step of the filtering pipeline into
    the respective corpora"""

    def run_for_corpus(self, workspace: Workspace, corpus_id: int):
        self.filter(workspace, corpus_id)


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