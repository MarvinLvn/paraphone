import random
import shutil
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Set, Tuple, List, Dict

import Levenshtein
from tqdm import tqdm

from paraphone.tasks.dictionaries import DictionaryCSV
from paraphone.tasks.filters.base import FilteringTaskMixin, CandidatesPairCSV, WordPair, CorpusFinalFilteringTask
from paraphone.tasks.phonemize import PhonemizedWordsCSV
from paraphone.tasks.tokenize import TokenizedWordsCSV
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
        # cleaning up folder in case of re-initing
        shutil.rmtree(steps_folder)
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

        previous_step_csv = self.previous_step_csv(workspace)
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


class MostFrequentHomophoneFilterTask(FilteringTaskMixin):
    """Filter homophones based on grapheme frequency (the grapheme with
    the largest frequency is the one that "wins" and other graphemic forms
    are filtered out) """
    requires = [
        "datasets/tokenized/all.csv"
    ]
    step_name = "homophones"

    def __init__(self):
        super().__init__()
        # phonetic forms and the frequency of the word that is associated with it
        # for two words with the same word_pho (homophones), only the one
        # with the most frequent occurences is kept
        self.word_pho_freq: Dict[str, int] = defaultdict(int)
        # word_pho -> word
        self.kept_word_pho: Dict[str, str] = dict()
        self.kept_words: Set[str] = set()

    def keep_pair(self, word_pair: WordPair) -> bool:
        return word_pair.word in self.kept_words

    def run(self, workspace: Workspace):
        tokenized_words_csv = TokenizedWordsCSV(
            workspace.tokenized / Path("all.csv")
        )
        tokenized_words = tokenized_words_csv.to_dict()
        previous_step_csv = self.previous_step_csv(workspace)
        for word, word_pho, _ in previous_step_csv:
            if self.word_pho_freq[word_pho] < tokenized_words[word]:
                self.word_pho_freq[word_pho] = tokenized_words[word]
                self.kept_word_pho[word_pho] = word

        self.kept_words = set(self.kept_word_pho.values())
        self.filter(workspace)


class WuggyHomophonesFilterTask(FilteringTaskMixin):
    """Filters out fake word whose phonetic form matches that of a real word."""
    requires = [
        "dictionaries/*/dict_folded.csv",
        "phonemized/all.csv"
    ]
    step_name = "wuggy-homophones"

    def __init__(self):
        super().__init__()
        self.all_words_phonemized: Set[str] = set()

    def keep_pair(self, word_pair: WordPair) -> bool:
        return word_pair.fake_word_pho not in self.all_words_phonemized

    def run(self, workspace: Workspace):
        self.all_words_phonemized = {}
        for dict_filepath in workspace.dictionaries.glob("**/dict_folded.csv"):
            dict_csv = DictionaryCSV(dict_filepath)
            self.all_words_phonemized.update({
                " ".join(pho) for _, pho, _ in dict_csv
            })
        phonemized_words_csv = PhonemizedWordsCSV(workspace.phonemized / Path("all.csv"))
        self.all_words_phonemized.update({
            " ".join(pho) for _, pho in phonemized_words_csv
        })

        self.filter(workspace)


class LevenshteinFilterTask(FilteringTaskMixin):
    """Keeps pairs whose edit distance is lower than a threshold"""
    step_name = "levenshtein"

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
