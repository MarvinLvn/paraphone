from collections import Counter, defaultdict
from typing import Dict, Tuple, List, Union

import numpy as np

from ..utils import consecutive_pairs, Phoneme

Ngram = Union[str, Tuple[str, str]]


class NGramComputer:

    def __init__(self, phonemic_freq: Dict[Tuple[Phoneme], int]):
        self.phonemic_freq = phonemic_freq

    def phonemes_freq_iter(self, bounded: bool):
        for phonemes, freq in self.phonemic_freq:
            if bounded:
                phonemes = ["_"] + phonemes + ["_"]
            for _ in range(freq):
                yield from phonemes

    def bigrams(self, bounded: bool):
        # compute unigrams and bigrams counters
        unigrams = Counter(self.phonemes_freq_iter(bounded=bounded))
        bigrams = Counter(consecutive_pairs(self.phonemes_freq_iter(bounded=bounded)))

        # normalize bigrams by the unigram of their first character
        return {
            bigram_char: bigram_count / unigrams[bigram_char[0]]
            for bigram_char, bigram_count in bigrams.items()
        }

    def unigrams(self, bounded: bool):
        # compute unigrams
        unigrams = Counter(self.phonemes_freq_iter(bounded=bounded))
        # normalize each unigram by the total sum of the counts
        unigrams_total = sum(unigrams.values())
        return {
            unigram_char: unigram_count / unigrams_total
            for unigram_char, unigram_count in unigrams
        }

    @classmethod
    def to_ngram_logprob(cls, ngrams: List[Ngram],
                         ngrams_freqs: Dict[Ngram, int]):
        ngrams_freqs = defaultdict(int, ngrams_freqs)
        ngram_values = np.array(ngrams_freqs[ngram] for ngram in ngrams)
        return np.sum(np.log(ngram_values))

    def ngram_scores(self, phonemes: List[Phoneme]):
        phonemes_bounded = ["_"] + phonemes + ["_"]

        scores = {}
        scores["unigram_unbounded"] = self.to_ngram_logprob(
            phonemes_bounded, self.unigrams(bounded=False)
        )
        scores["unigram_bounded"] = self.to_ngram_logprob(
            phonemes, self.unigrams(bounded=True)
        )
        scores["bigram_unbounded"] = self.to_ngram_logprob(
            consecutive_pairs(phonemes_bounded), self.bigrams(bounded=False)
        )
        scores["bigram_bounded"] = self.to_ngram_logprob(
            consecutive_pairs(phonemes), self.bigrams(bounded=True)
        )
