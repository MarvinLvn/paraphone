import re
from collections import Counter
from pathlib import Path
from typing import Set, List, Literal

from tqdm import tqdm

from .base import BaseTask, CorporaTaskMixin
from .filters.base import CandidatesPairCSV
from .imports import FileID
from .tokenize import TokenizedWordsCSV
from ..utils import logger
from ..workspace import Workspace, WorkspaceCSV


class CorporaCreationTask(BaseTask):
    """Creates corpora based on the list of tokenized words for each text
    in the dataset and the text files families. The output words list are
    compted from the intersection of the words of the texts from the families'
    groups"""
    requires = [
        "datasets/tokenized/all.csv",
        "datasets/families/*/*.txt"
    ]

    creates = [
        "corpora/",
        "corpora/tokenized/*.csv"
    ]

    def load_group_words(self, group: Set[FileID], workspace: Workspace) -> Counter:
        tokenized_folder = workspace.root_path / Path("datasets/tokenized/per_text/")

        group_words = Counter()
        for file_id in group:
            tokenized_file = TokenizedWordsCSV(tokenized_folder / Path(f"{file_id}.csv"))
            try:
                group_words.update(tokenized_file.to_dict())
            except FileNotFoundError:
                logger.warning(f"Couldn't find tokenized text file {file_id}.csv")
                continue

        return group_words

    @staticmethod
    def sort_families_folders(folders: List[Path]) -> List[Path]:
        return sorted(folders, key=lambda x: int(x.stem.split("_")[1]),
                      reverse=True)

    def run(self, workspace: Workspace):
        workspace.corpora.mkdir(parents=True, exist_ok=True)

        logger.info("Building families words lists...")
        families_folder = workspace.datasets / Path("families/")
        # Families *have* to be sorted
        pbar = tqdm(self.sort_families_folders(families_folder.iterdir()))
        previous_group_words: Set[str] = set()
        for family_folder in pbar:
            family_folder: Path
            assert family_folder.is_dir()
            family_id = int(re.fullmatch(r"family_([0-9]+)", family_folder.name)[1])
            assert len(list(family_folder.iterdir())) == family_id

            family_words_dict = None
            groups_paths = self.sort_families_folders(list(family_folder.iterdir()))
            for group_filepath in groups_paths:
                pbar.set_description(f"Family {family_id}: {group_filepath.name}")

                assert group_filepath.is_file()
                with open(group_filepath) as group_file:
                    group = set(file_id for file_id in group_file.read().split("\n") if file_id)
                group_words_dict = self.load_group_words(group, workspace)

                # the first group is taken as base for the intersection
                if family_words_dict is None:
                    family_words_dict = group_words_dict
                else:
                    # else we only keep the words set that intersect
                    # and increment their counts with the currentgroup's count
                    words_intersection = (set(group_words_dict) & set(family_words_dict))
                    for word in list(family_words_dict):
                        if word in words_intersection:
                            family_words_dict[word] += group_words_dict[word]
                        else:
                            del family_words_dict[word]

            # removing words from "previous" family
            len_before = len(family_words_dict)
            for word in previous_group_words:
                if word in family_words_dict:
                    del family_words_dict[word]
            previous_group_words.update(set(family_words_dict.keys()))

            logger.debug(f"Removed {len_before - len(family_words_dict)} words "
                         f"from smaller corpora in corpus {family_id}")

            logger.debug(f"Writing words list for family {family_id}")
            tokenized_corpora_folder = workspace.corpora / Path("tokenized/")
            tokenized_corpora_folder.mkdir(parents=True, exist_ok=True)
            family_words_csv = TokenizedWordsCSV(tokenized_corpora_folder /
                                                 Path(f"corpus_{family_id}.csv"))
            # sorting dict by word count (decreasing)
            family_words_dict = dict(sorted(family_words_dict.items(),
                                            key=lambda item: item[1],
                                            reverse=True))
            with family_words_csv.dict_writer as dict_writer:
                dict_writer.writeheader()
                for word, count in family_words_dict.items():
                    dict_writer.writerow({
                        "word": word, "count": count
                    })


class ZeroSpeechCSV(WorkspaceCSV):
    headers = ["id", "filename", "voice", "frequency", "word", "phones", "length", "correct"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator=",", header=self.header)


class MakeZeroSpeechTable(BaseTask, CorporaTaskMixin):
    requires = [
        "corpora/wuggy_pairs/*",
        "synth/audio/phonetic/",
        "synth/audio/text/",
    ]

    creates = [
        "corpora/zerospeech/",
        "corpora/zerospeech/corpus_*.csv"
    ]

    def __init__(self, real_word_synth: Literal["text", "phonetic"]):
        super().__init__()
        self.real_word_synth = real_word_synth

    def create_zr_corps_csv(self,
                            wuggy_pairs_path: Path,
                            zr_csv_path : Path,
                            workspace: Workspace):
        phonetic_synth_folder = workspace.synth / Path("audio/phonetic/")
        text_synth_folder = workspace.synth / Path("audio/text/")

        corpus_wuggy_pairs_csv = CandidatesPairCSV(wuggy_pairs_path)
        zr_corpus_csv = ZeroSpeechCSV(zr_csv_path)
        with zr_corpus_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for word, phonetic, fake_phonetic in corpus_wuggy_pairs_csv:
                dict_writer.writerow(

                )

    def run(self, workspace: Workspace):
        zr_folder = workspace.corpora / Path("zerospeech")
        zr_folder.mkdir(parents=True, exist_ok=True)
        wuggy_pairs_folder = workspace.corpora / Path("wuggy_pairs")

        for corpus_id, corpus_csv_path in self.find_corpora(wuggy_pairs_folder):
            logger.info(f"For corpus {corpus_id}")
            self.create_zr_corps_csv(
                corpus_csv_path,
                zr_folder / Path(f"corpus_{corpus_id}.csv"),
                workspace
            )


