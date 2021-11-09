import logging
import re
from collections import Counter
from pathlib import Path
from typing import Set

from tqdm import tqdm

from .base import BaseTask
from .imports import FileID
from .tokenize import TokenizedTextCSV
from ..utils import logger
from ..workspace import Workspace


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
        "corpora/*.csv"
    ]

    def load_group_words(self, group: Set[FileID], workspace: Workspace) -> Counter:
        tokenized_folder = workspace.root_path / Path("datasets/tokenized/per_text/")

        group_words = Counter()
        for file_id in group:
            tokenized_file = TokenizedTextCSV(tokenized_folder / Path(f"{file_id}.csv"))
            try:
                group_words.update(tokenized_file.to_dict())
            except FileNotFoundError:
                logger.warning(f"Couldn't find tokenized text file {file_id}.csv")
                continue

        return group_words

    def run(self, workspace: Workspace):
        workspace.corpora.mkdir(parents=True, exist_ok=True)

        logger.info("Building families words lists...")
        families_folder = workspace.datasets / Path("families/")
        pbar = tqdm(list(families_folder.iterdir()))
        for family_folder in pbar:
            family_folder: Path
            assert family_folder.is_dir()
            family_id = int(re.fullmatch(r"family_([0-9]+)", family_folder.name)[1])
            assert len(list(family_folder.iterdir())) == family_id

            family_words_dict = None
            for group_filepath in family_folder.iterdir():
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

            logger.debug(f"Writing words list for family {family_id}")
            family_words_csv = TokenizedTextCSV(workspace.corpora /
                                                Path(f"corpus_{family_id}.csv"))
            with family_words_csv.dict_writer as dict_writer:
                dict_writer.writeheader()
                for word, count in family_words_dict.items():
                    dict_writer.writerow({
                        "word": word, "count": count
                    })
