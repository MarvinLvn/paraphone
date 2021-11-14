import random
from collections import Counter, defaultdict
from typing import Dict, Tuple, List, Union, Iterable, Callable, Optional, Any
from typing_extensions import Literal

import numpy as np

from .utils import consecutive_pairs, Phoneme

Ngram = Union[str, Tuple[str, str]]


class NGramComputer:

    def __init__(self, phonemic_freq: Dict[Tuple[Phoneme], int]):
        self.phonemic_freq = phonemic_freq

    def phonemes_freq_iter(self, bounded: bool, ngram_type: Literal["unigram", "bigram"]) -> Iterable[Dict]:
        for phonemes, freq in self.phonemic_freq.items():
            if bounded:
                phonemes = ["_"] + list(phonemes) + ["_"]

            if ngram_type == "unigram":
                ngrams = phonemes
            else:
                ngrams = consecutive_pairs(phonemes)
            ngram_counter = Counter(ngrams)
            yield {ngram: value * freq for ngram, value in ngram_counter.items()}

    def bigrams(self, bounded: bool):
        # compute unigrams and bigrams counters
        unigrams = Counter()
        for freq_dict in self.phonemes_freq_iter(bounded=bounded, ngram_type="unigram"):
            unigrams.update(freq_dict)

        bigrams = Counter()
        for freq_dict in self.phonemes_freq_iter(bounded=bounded, ngram_type="bigram"):
            bigrams.update(freq_dict)

        # normalize bigrams by the unigram frequency of their first character
        return {
            bigram_char: bigram_count / unigrams[bigram_char[0]]
            for bigram_char, bigram_count in bigrams.items()
        }

    def unigrams(self, bounded: bool):
        # compute unigrams
        unigrams = Counter()
        for freq_dict in self.phonemes_freq_iter(bounded=bounded, ngram_type="unigram"):
            unigrams.update(freq_dict)

        # normalize each unigram by the total sum of the counts
        unigrams_total = sum(unigrams.values())
        return {
            unigram_char: unigram_count / unigrams_total
            for unigram_char, unigram_count in unigrams.items()
        }

    @classmethod
    def to_ngram_logprob(cls, ngrams: List[Ngram],
                         ngrams_probabilities: Dict[Ngram, int]):
        """Computes the log of the product of ngram probabilities, via a sum of logs"""
        # nonexistent values are 0.0, via the default dict
        ngrams_probabilities = defaultdict(float, ngrams_probabilities)
        ngram_values = np.array([ngrams_probabilities[ngram] for ngram in ngrams])
        return np.sum(np.log(ngram_values))


Score = float


def abs_sum_score_fn(scores: Iterable[Score]) -> float:
    return np.abs(np.array(list(scores)) - 0.5).sum()


FREQS_RANKS = [0, 10, 20, 50, 100]  # TODO: ask about that


def rank(frequency: int):
    if frequency == 0:
        return "FREQRANK[0]"
    min_freq, max_freq = FREQS_RANKS[0], FREQS_RANKS[-1]
    for low, high in consecutive_pairs(FREQS_RANKS):
        if low <= frequency <= high:
            yield f"FREQRANK[{low} - {high}]"
    else:
        if frequency > max_freq:
            f"FREQRANK[{max_freq} - inf]"


class ScoringStatistics:

    def __init__(self, scores_names: List[str]):
        # number of words that have been chosen
        self.current_words_counter = 1
        # for each score, the sum of balancing scores
        self.aggregate_scores = {score: 0.0 for score in scores_names}

    @property
    def current_scores(self):
        return {score_name: aggregate_score / self.current_words_counter
                for score_name, aggregate_score in self.aggregate_scores.items()}

    def compare_scores(self, real_word_score: float, fake_word_score: float):
        if real_word_score > fake_word_score:
            return 1.
        elif real_word_score == fake_word_score:
            return 0.5
        else:
            return 0.

    def compute_candidate_scores(self,
                                 real_word_scores: Dict[str, float],
                                 fake_word_scores: Dict[str, float]):
        scores: Dict[str, float] = dict()
        for score_name in self.aggregate_scores:
            score_comparison = self.compare_scores(real_word_scores[score_name],
                                                   fake_word_scores[score_name])
            updated_aggregated_score = self.aggregate_scores[score_name] + score_comparison
            candidate_score = (updated_aggregated_score / (self.current_words_counter + 1))
            scores[score_name] = candidate_score
        return scores

    def update_scores_stats(self, candidate_scores: Dict[str, float]):
        for score_name, score in candidate_scores.items():
            self.aggregate_scores[score_name] += score
        self.current_words_counter += 1


WordCategory = Tuple[Any, ...]


class FakeWordsBalancer:

    def __init__(self,
                 words_scores: Dict[str, Dict[str, Score]],  # {word_pho : {score_name : score}}
                 word_categories: Dict[str, WordCategory],  # { word_pho : (cat_1, cat_2,...)}
                 word_nonword_pairs: Dict[str, List[str]],  # {real_word : list(fake_word) }
                 objective_fn: Optional[Callable[[Iterable[Score]], float]] = None):
        self.objective_fn = objective_fn if objective_fn is not None else abs_sum_score_fn
        self.words_scores = words_scores
        self.word_categories = word_categories
        self.word_nonword_pairs = word_nonword_pairs

        # all categories
        self.categories = set(word_categories.values())
        # all score names
        self.score_names = list(words_scores[list(words_scores)[0]].keys())
        # 1 scoring statistic per category
        self.categories_stats: Dict[WordCategory, ScoringStatistics] = {
            cat: ScoringStatistics(self.score_names) for cat in self.categories
        }

    def choose_non_word(self, real_word: str) -> str:
        # retrieving the scores statistics for the current word's category
        scores_stats = self.categories_stats[self.word_categories[real_word]]
        current_objective = self.objective_fn(scores_stats.current_scores.values())

        fake_word_candidate_scores = dict()
        # for each fake word, computing its scores compared to the current
        # statistics
        for fake_word in self.word_nonword_pairs[real_word]:
            candidate_scores = scores_stats.compute_candidate_scores(
                self.words_scores[real_word],
                self.words_scores[fake_word]
            )
            fake_word_candidate_scores[fake_word] = candidate_scores

            candidate_obj = self.objective_fn(candidate_scores.values())
            if candidate_obj < current_objective:
                chosen_fake_word = fake_word
                break
        else:
            chosen_fake_word = random.choice(self.word_nonword_pairs[real_word])

        if chosen_fake_word is None:
            raise RuntimeError()
        else:
            scores_stats.update_scores_stats(
                fake_word_candidate_scores[chosen_fake_word])
            return chosen_fake_word

    def iter_balanced_pairs(self) -> Iterable[Tuple[str, str]]:
        real_words = list(self.word_nonword_pairs.keys())
        random.shuffle(real_words)
        for real_word in real_words:
            yield real_word, self.choose_non_word(real_word)
