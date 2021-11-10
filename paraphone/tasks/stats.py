from collections import defaultdict
from pathlib import Path
from typing import Iterable, Tuple

from paraphone.ngram_tools.ngrams import NGramComputer
from paraphone.tasks.base import BaseTask, CorporaTaskMixin
from paraphone.tasks.phonemize import PhonemizedWordsCSV
from paraphone.tasks.tokenize import TokenizedTextCSV
from paraphone.utils import logger
from paraphone.workspace import Workspace, WorkspaceCSV


class NgramsProbCSV(WorkspaceCSV):
    header = ["ngram", "probability"]

    def __init__(self, file_path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str, float]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield row["ngram"], float(row["probability"])


class CorporaNgramStatsTask(BaseTask, CorporaTaskMixin):
    requires = [
        "phonemized/all.csv",
        "corpora/*.csv"
    ]

    creates = {
        "stats/corpora/ngrams/*/*.csv",
        "stats/corpora/ngram/"
    }

    def run(self, workspace: Workspace):
        corpora_ngrams_folder = workspace.stats / Path("corpora/ngrams/")
        corpora_ngrams_folder.mkdir(parents=True, exist_ok=True)
        phonemized_words_csv = PhonemizedWordsCSV(workspace.phonemized / Path("all.csv"))
        phonemized_words = phonemized_words_csv.to_dict()
        for corpus_id, tokenized_corpus_path in self.iter_corpora(workspace.corpora):
            logger.info(f"Statistics for corpus {corpus_id}")

            logger.info("Loading frequency for each phonetic form")
            corpus_phon_freqs = defaultdict(int)
            for word, freq in TokenizedTextCSV(tokenized_corpus_path):
                try:
                    corpus_phon_freqs[tuple(phonemized_words[word])] += freq
                except KeyError as err:
                    print(err)

            ngram_computer = NGramComputer(corpus_phon_freqs)

            corpus_stats_folder = corpora_ngrams_folder / Path(f"corpus_{corpus_id}/")
            corpus_stats_folder.mkdir(parents=True, exist_ok=True)
            ngrams = {
                "unigram_bounded": ngram_computer.unigrams(bounded=True),
                "unigram_unbounded": ngram_computer.unigrams(bounded=False),
                "bigram_bounded": ngram_computer.bigrams(bounded=True),
                "bigram_unbounded": ngram_computer.bigrams(bounded=False),
            }
            for ngram_name, ngram_dict in ngrams.items():
                filepath = corpus_stats_folder / Path(f"{ngram_name}.csv")
                with NgramsProbCSV(filepath).dict_writer as dict_writer:
                    dict_writer.writeheader()
                    for ngram, prob in ngram_dict.items():
                        if isinstance(ngram, tuple):
                            ngram = " ".join(ngram)
                        dict_writer.writerow({"ngram": ngram, "probability": prob})
