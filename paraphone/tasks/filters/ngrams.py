from pathlib import Path
from typing import Iterable, List, Tuple, Set

from tqdm import tqdm

from .base import BaseFilteringTask
from ..base import BaseTask
from ..syllabify import SyllabifiedWordsCSV
from ..tokenize import TokenizedTextCSV
from ..wuggy_gen import FakeWordsCandidatesCSV
from ...ngram_tools.ngrams import NGramComputer
from ...utils import logger, Phoneme, consecutive_pairs, count_lines
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
        "phonemized/syllabic.csv",
        "wuggy/candidates.csv",
        "candidates_filtering/steps/*"
    ]

    creates = [
        "candidates_filtering/ngram/phonemized_words_frequencies.csv",
        "candidates_filtering/ngram/scores.csv",
    ]

    def run(self, workspace: Workspace):
        # NOTES :
        # freqseq = frequency of each phonemized word (phon_ipa, freq)
        # input_file = phonemized word/ phonemized fake word couples

        # firstly, generate the phonemized word -> frequency csv
        # from the syllabic CSV (some useless words are filtered out)
        syllabic_csv = SyllabifiedWordsCSV(workspace.phonemized / Path("all.csv"))
        frequency_csv = PhonemizedWordsFrequencyCSV(workspace.candidates_filtering
                                                    / Path("ngram/phonemized_words_frequencies.csv"))
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
        candidates_csv = FakeWordsCandidatesCSV(self.previous_step_filepath(workspace))
        ngrams_scores_csv = NgramScoresCSV(workspace.candidates_filtering / Path("ngrams/scores.csv"))
        candidates_count = count_lines(self.previous_step_filepath(workspace))
        phonetic_forms: Set[Tuple[str]] = set()
        with ngrams_scores_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for _, phonetic, _, fake_phonetic, _ in tqdm(candidates_csv, total=candidates_count):
                for phonemes in (phonetic, fake_phonetic):
                    if tuple(phonemes) in phonetic_forms:
                        continue

                    phonemes_bounded = ["_"] + phonemes + ["_"]
                    row = {"phonetic": phonemes,
                           "unigram_unbounded": ngram_computer.to_ngram_logprob(
                               phonemes_bounded, unigram_unbounded),
                           "unigram_bounded": ngram_computer.to_ngram_logprob(
                            phonemes, unigram_bounded),
                           "bigram_unbounded": ngram_computer.to_ngram_logprob(
                            consecutive_pairs(phonemes_bounded), bigrams_unbounded),
                           "bigram_bounded": ngram_computer.to_ngram_logprob(
                            consecutive_pairs(phonemes), bigrams_bounded)
                           }
                    dict_writer.writerow(row)


class NgramBuildCategoriesTask(BaseFilteringTask):

    def run(self, workspace: Workspace):
        parser = argparse.ArgumentParser()
        parser.add_argument('path_pairs', help='Path to the file with ARPA pairs of words/non-words')
        parser.add_argument('freq_path', help='Path to output matching words')
        parser.add_argument('output_path', help='Type of token that is being processed', type=str)
        args = parser.parse_args()

        f = open(args.freq_path, "r")
        i = open(args.path_pairs, "r")
        g = open(args.output_path, "w+")
        m = f.readlines()
        n = i.readlines()
        d = {}
        c = 0
        for elem in m:
            d[elem.split('\t')[0].replace('1', '0')] = elem.split('\t')[1].split('\n')[0]
        words = []
        for j in range(1, len(n)):
            words.append(n[j].split('\t')[0])
        s = set(words)
        g.write("Word" + '\t' + 'length_phone' + '\t' + 'freq' + '\n')
        for w in s:
            try:
                g.write(w + '\t' + str(len(w.split(' '))) + '\t' + str(d[w.replace('1', '0')]) + '\n')
            except KeyError:
                g.write(w + '\t' + str(len(w.split(' '))) + '\t' + str(1) + '\n')
                print(w)
                c += 1
        print('Unrecognized word : {} out of '.format(c) + str(len(s)))
        g.close()
        f.close()
        i.close()


class NgramBalanceScoresTask(BaseTask):
    pass
