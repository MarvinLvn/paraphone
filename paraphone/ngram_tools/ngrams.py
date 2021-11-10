from collections import Counter, defaultdict
from typing import Dict, Tuple, List, Union, Iterable, Literal

import numpy as np

from ..utils import consecutive_pairs, Phoneme

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
                         ngrams_freqs: Dict[Ngram, int]):
        ngrams_freqs = defaultdict(int, ngrams_freqs)
        ngram_values = np.array(ngrams_freqs[ngram] for ngram in ngrams)
        return np.sum(np.log(ngram_values))
