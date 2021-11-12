from collections import defaultdict
from pathlib import Path
from typing import Iterable, List, Tuple, Set

from tqdm import tqdm

from .base import BaseFilteringTask, CandidatesPairCSV, WordPair
from ..syllabify import SyllabifiedWordsCSV
from ..tokenize import TokenizedTextCSV
from ...ngrams_tools import NGramComputer, FakeWordsBalancer, abs_sum_score_fn, rank
from ...utils import logger, Phoneme, consecutive_pairs
from ...workspace import Workspace, WorkspaceCSV

# TODO : file names for the filtering steps are named
#  step[x]_[operation].csv #
WordPhonemesFrequency = Iterable[Tuple[List[Phoneme], int]]


class NgramScoresCSV(WorkspaceCSV):
    header = ['phonetic',
              'unigram_bounded', 'unigram_unbounded',
              'bigram_bounded', 'bigram_unbounded']

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)


class PhonemizedWordsFrequencyCSV(WorkspaceCSV):
    header = ['word', 'phonetic', 'frequency']

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[List[Phoneme], int]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield (row["word"],
                       row["phonetic"].split(" "),
                       int(row["frequency"]))


class NgramScoringTask(BaseFilteringTask):
    requires = [
        "datasets/tokenized/all.csv",  # used for word frequency (not normalized)
        "wuggy/candidates.csv",
        "candidates_filtering/steps/*"
    ]

    creates = [
        "candidates_filtering/ngram/phonemized_words_frequencies.csv",
        "candidates_filtering/ngram/scores.csv",
    ]

    def run(self, workspace: Workspace):
        # TODO: comment this for E.D.
        ngram_data_folder = workspace.candidates_filtering / Path("ngram/")
        ngram_data_folder.mkdir(parents=True, exist_ok=True)
        # firstly, generate the {phonemized word -> frequency} csv
        # from the syllabic CSV (some useless words are filtered out)
        syllabic_csv = SyllabifiedWordsCSV(workspace.phonemized / Path("syllabic.csv"))
        frequency_csv = PhonemizedWordsFrequencyCSV(ngram_data_folder
                                                    / Path("phonemized_words_frequencies.csv"))
        tokenized_csv = TokenizedTextCSV(workspace.tokenized / Path("all.csv"))
        words_freq = tokenized_csv.to_dict()
        with frequency_csv.dict_writer as freq_writer:
            freq_writer.writeheader()
            for word, phon, syll in syllabic_csv:
                freq_writer.writerow({
                    "word": word,
                    "phonetic": " ".join(phon),
                    "frequency": words_freq[word]
                })

        logger.info("Computing ngrams probabilities over the phonemized dataset")
        phonemes_freqs = {tuple(phonetic): frequency for _, phonetic, frequency in frequency_csv}
        ngram_computer = NGramComputer(phonemes_freqs)
        bigrams_bounded = ngram_computer.bigrams(bounded=True)
        bigrams_unbounded = ngram_computer.bigrams(bounded=False)
        unigram_bounded = ngram_computer.unigrams(bounded=True)
        unigram_unbounded = ngram_computer.unigrams(bounded=False)

        logger.info("Computing ngram scores over the wuggy real words/fake words pairs")
        last_step_path, last_step_id = self.previous_step_filepath(workspace)
        candidates_csv = CandidatesPairCSV(last_step_path)
        ngrams_scores_csv = NgramScoresCSV(ngram_data_folder / Path("scores.csv"))
        phonetic_forms: Set[Tuple[str]] = set()
        with ngrams_scores_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for _, word_pho, fake_word_pho in tqdm(candidates_csv,
                                                   total=candidates_csv.lines_count):
                for phonemes in (word_pho, fake_word_pho):
                    phonemes = phonemes.split(" ")
                    if tuple(phonemes) in phonetic_forms:
                        continue

                    phonemes_bounded = ["_"] + phonemes + ["_"]
                    row = {"phonetic": " ".join(phonemes),
                           "unigram_unbounded": ngram_computer.to_ngram_logprob(
                               phonemes, unigram_unbounded),
                           "unigram_bounded": ngram_computer.to_ngram_logprob(
                               phonemes_bounded, unigram_bounded),
                           "bigram_unbounded": ngram_computer.to_ngram_logprob(
                               consecutive_pairs(phonemes), bigrams_unbounded),
                           "bigram_bounded": ngram_computer.to_ngram_logprob(
                               consecutive_pairs(phonemes_bounded), bigrams_bounded)
                           }
                    dict_writer.writerow(row)
                    phonetic_forms.add(tuple(phonemes))


class NgramBalanceScoresTask(BaseFilteringTask):
    """Use computed ngram scores to select only one fake word candidate per
    real word"""

    requires = [
        "candidates_filtering/ngram/phonemized_words_frequencies.csv",
        "candidates_filtering/ngram/scores.csv",
    ]

    def __init__(self):
        super().__init__()
        self.chosen_pairs: Set[Tuple[str, str]] = set()

    def filter_fn(self, word_pair: WordPair) -> bool:
        return (word_pair.word_pho, word_pair.fake_word_pho) in self.chosen_pairs

    def run(self, workspace: Workspace):
        ngram_data_folder = workspace.candidates_filtering / Path("ngram")
        freqs_csv = PhonemizedWordsFrequencyCSV(ngram_data_folder / Path("phonemized_words_frequencies.csv"))
        scores_csv = NgramScoresCSV(ngram_data_folder / Path("scores.csv"))

        #
        categories = {
            " ".join(word_pho): (len(word_pho), rank(freq))
            for _, word_pho, freq in freqs_csv
        }
        scores = {}  # phonetic_form -> ngram scores (unigram, bigram, etc...)
        with scores_csv.dict_reader as dict_reader:
            for row in dict_reader:
                phonetic = row.pop("phonetic")
                scores[phonetic] = {score_name: float(score)
                                    for score_name, score in row.items()}

        previous_step_csv_path, previous_step_id = self.previous_step_filepath(workspace)
        previous_step_csv = CandidatesPairCSV(previous_step_csv_path)
        word_nonword = defaultdict(list)  # word -> list(nonwords)
        for _, word_pho, fake_word_pho in previous_step_csv:
            word_nonword[word_pho].append(fake_word_pho)

        balancer = FakeWordsBalancer(words_scores=scores,
                                     word_categories=categories,
                                     word_nonword_pairs=word_nonword,
                                     objective_fn=abs_sum_score_fn)

        logger.info("Finding a balanced nonword candidate for each word")
        for word, fake_word in tqdm(balancer.iter_balanced_pairs(), total=len(word_nonword)):
            self.chosen_pairs.add((word, fake_word))

        self.filter(workspace, self.filter_fn)
