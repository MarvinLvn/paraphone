import abc
import re
from collections import OrderedDict
from dataclasses import dataclass
from os import symlink
from pathlib import Path
from shutil import copytree
from typing import Literal, Dict, Type, Tuple, Iterable, Text, Set

import yaml
from sortedcontainers import SortedDict

from .base import BaseTask
from ..utils import logger, pairwise
from ..workspace import Workspace, WorkspaceCSV

Importers = Literal["librivox", "littaudio"]
FileID = Text
FamilyId = int


class BaseImporter(metaclass=abc.ABCMeta):
    id_regex = re.compile("")
    glob_pattern = "**/*.txt"

    def __init__(self, dataset_path: Path):
        self.dataset_path = dataset_path

    @classmethod
    def get_id_from_path(cls, path: Path):
        return cls.id_regex.match(path.stem)[1]

    def __iter__(self) -> Iterable[Tuple[FileID, Path]]:
        for file_path in self.dataset_path.glob(self.glob_pattern):
            file_id = self.get_id_from_path(file_path)
            yield file_id, file_path


class LibriVoxImporter(BaseImporter):
    id_regex = re.compile("(.+)_librivox_.*")
    glob_pattern = "*_librivox_*.txt"

    # removing the useless end of file to get file_id
    # eg : daughter_land_1101_librivox_64kb_mp3_text.txt -> daughter_land_1101


class LittAudioImporter(BaseImporter):
    id_regex = re.compile("[0-9]+_[0-9]+")
    glob_pattern = "**/*.txt"

    # removing the useless end of file to get file_id
    # eg : 9357_1690.txt -> 9357_1690


@dataclass
class DatasetIndexCSV(WorkspaceCSV):
    """CSV that stores an index of the file ID -> file path index.
    The file path is relative to the workspace
    """
    header = ["file_id", "file_path"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)
        self._entries = OrderedDict()

    def add_entry(self, file_id: str, file_path: Path):
        if self.file_path.exists() and not self._entries:
            self._entries.update(self.to_dict())
        assert file_id not in self._entries
        self._entries[file_id] = file_path

    def write_entries(self):
        self.write([{"file_id": file_id, "file_path": file_path}
                    for file_id, file_path in self._entries.items()])

    def __iter__(self) -> Iterable[Tuple[FileID, Path]]:
        dataset_path = self.file_path.parent
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                path = (dataset_path / Path(row["file_path"]))
                assert path.exists()
                yield row["file_id"], path

    def to_dict(self):
        return {file_id: file_path for file_id, file_path in self}


class DatasetMetadataCSV(WorkspaceCSV):
    """Contains the family ID, language and dataset for each file, along
    with a bunch of data we don't need here"""

    def __init__(self, file_path: Path, language: str):
        super().__init__(file_path, separator=",", header=None)
        assert language in {"fr", "en"}
        self.language = language

    def __iter__(self) -> Iterable[Tuple[FileID, FamilyId]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                file_path = Path(row["text_path"])
                file_lang: str = row["language"].lower()
                file_family = int(row["family_id"])
                file_book_id: str = row["book_id"].lower()

                if file_lang != self.language:
                    continue

                if "librivox" in file_book_id:
                    file_id = LibriVoxImporter.get_id_from_path(file_path)
                elif "litteratureaudio" in file_book_id:
                    file_id = LibriVoxImporter.get_id_from_path(file_path)
                else:
                    logger.warning(f"Couldn't get book id for file {file_path.stem}")
                    continue

                yield file_id, file_family


class DatasetImportTask(BaseTask):
    """Imports a dataset (Librivox-type or AudioLitt-type) in the workspace
    folder, by copying or symlinking it. Parses all the dataset files to
    build an index of the files in dataset/index.csv"""

    requires = [
        "datasets/"
    ]
    creates = [
        "datasets/index.csv",
        "datasets/raw/{dataset_name}/*.txt"
    ]

    importers: Dict[Importers, Type[BaseImporter]] = {
        "littaudio": LittAudioImporter,
        "librivox": LibriVoxImporter
    }

    def __init__(self, dataset_path: Path,
                 dataset_type: Importers,
                 copy=False, symlink=True):
        super().__init__()
        assert not (copy and symlink)
        self.copy = copy
        self.symlink = symlink
        self.importer = self.importers[dataset_type](dataset_path)
        self.dataset_path = dataset_path

    def run(self, workspace: Workspace):
        assert self.dataset_path.exists()

        # either copying of symlinking dataset
        raw_dataset_path = workspace.root_path / Path("datasets/raw/")
        raw_dataset_path.mkdir(parents=True, exist_ok=True)
        if self.copy:
            logger.info(f"Copying {self.dataset_path}")
            dataset_copy_path = raw_dataset_path / Path(self.dataset_path.name)
            copytree(self.dataset_path, dataset_copy_path, dirs_exist_ok=True)

        if self.symlink:
            logger.debug(f"Symlinking {self.dataset_path}")
            dataset_syml_path = raw_dataset_path / Path(self.dataset_path.name)
            symlink(self.dataset_path.absolute(), dataset_syml_path)
        logger.info("Done.")

        # computing the file index (file_id -> file path relative to workspace)
        idx_file = workspace.root_path / Path("datasets/index.csv")
        logger.info("Updating file index...")
        dataset_csv = DatasetIndexCSV(idx_file)
        for file_id, file_path in self.importer:
            dataset_csv.add_entry(file_id, file_path)
        dataset_csv.write_entries()
        logger.info("Update done")


class FamiliesImportTask(BaseTask):
    """Parses the so-called 'matched2.csv' metadata csv to extract the
    'primary' family of each text file. From this, builds the larger families
    by grouping these primary families of text files."""

    requires = [
        "datasets/index.csv",
        "config.csv"
    ]

    creates = [
        "datasets/families/",
        "datasets/families/*/*.csv",
    ]

    def __init__(self, families_filepath: Path):
        super().__init__()
        self.families_filepath = families_filepath

    def run(self, workspace: Workspace):
        with open(workspace.root_path / Path("config.csv")) as cfg_file:
            cfg = yaml.safe_load(cfg_file)
        lang = cfg["lang"]

        dataset_metadata_csv = DatasetMetadataCSV(self.families_filepath,
                                                  language=lang)

        logger.info(f"Loading families from {self.families_filepath}...")
        base_families: Dict[int, Set[FileID]] = SortedDict({i: set()
                                                            for i in range(64)})
        for file_id, family_id in dataset_metadata_csv:
            logger.debug(f"Found family {family_id} for file {file_id}")
            base_families[family_id].add(file_id)

        # TODO : maybe check for empty intersection
        max_fam_count = max(len(fam) for fam in base_families.values())
        min_fam_count = min(len(fam) for fam in base_families.values())
        logger.info(f"There are between {min_fam_count} and {max_fam_count} for"
                    f" all 64 primary families")

        logger.info("Building metafamilies unions...")

        metafamilies: Dict[int, Dict[int, Set[FileID]]] = SortedDict({64: base_families})
        metafamilies_ids = [2 ** i for i in range(0, 6)]

        for current_mf_id in reversed(metafamilies_ids):
            previous_mf_id = current_mf_id * 2
            metafamilies[current_mf_id] = SortedDict({i: set()
                                                      for i in range(current_mf_id)})

            families_couples = list(pairwise(metafamilies[previous_mf_id].values()))
            for fam_id, (fam_a, fam_b) in enumerate(families_couples):
                metafamilies_ids[current_mf_id][fam_id] = fam_a | fam_b

        # TODO: check that length of metafamily 1  == sum of lengths for families of metafam 64
