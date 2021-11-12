import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Tuple, List, Iterable

from tqdm import tqdm

from ..base import BaseTask
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


class BaseFilteringTask(BaseTask):
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

    def filter(self,
               input_csv: CandidatesPairCSV,
               output_csv: CandidatesPairCSV,
               criterion_fn: Callable[[WordPair], bool]):
        """Filters out candidates based on the criterion_fn's output"""
        logger.info(f"Filtering pairs from {input_csv.file_path} into {output_csv.file_path}")
        with output_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, word_pho, fake_word_pho in tqdm(input_csv, total=input_csv.lines_count):
                word_pair = WordPair(word, word_pho, fake_word_pho)
                if criterion_fn(word_pair):
                    dict_writer.writerow({
                        "word": word, "word_pho": word_pho, "fake_word_pho": fake_word_pho
                    })


class InitFilteringTask(BaseFilteringTask):
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


class RandomFilterTask(BaseFilteringTask):
    """Keeps a random split of the candidates determined by ratio."""
    step_name = "random"

    def __init__(self, ratio: float):
        super().__init__()
        assert 0 < ratio < 1.0
        self.ratio = ratio

    def filtering_fn(self, word_pair: WordPair):
        return random.random() < self.ratio

    def run(self, workspace: Workspace):
        random.seed(4577)

        previous_step_path, previous_step_id = self.previous_step_filepath(workspace)
        previous_step_csv = CandidatesPairCSV(previous_step_path)
        output_step_csv = CandidatesPairCSV(previous_step_path.parent
                                            / self.next_step_filename(previous_step_id))
        self.filter(previous_step_csv, output_step_csv, self.filtering_fn)
