import re
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple, List, Iterable, Set, Optional

from tqdm import tqdm

from ..base import BaseTask, CorporaTaskMixin
from ..tokenize import TokenizedWordsCSV
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
        logger.info(f"Kept {kept_count} ({kept_count / pairs_count:.0%}) of pairs.")


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
        logger.info(f"Kept {kept_count} ({kept_count / pairs_count:.0%}) of pairs for corpus {corpus_id}")

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


