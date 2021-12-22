import re
from collections import Counter
from pathlib import Path
from shutil import copyfile
from typing import Set, List, Literal, Iterable, Optional

import pandas
from sortedcontainers import SortedDict
from tqdm import tqdm

from .base import BaseTask, CorporaTaskMixin
from .filters.base import CandidatesPairCSV
from .imports import FileID
from .tokenize import TokenizedWordsCSV
from ..utils import logger
from ..workspace import Workspace, WorkspaceCSV


def load_group_words(group: Set[FileID], workspace: Workspace) -> Counter:
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
        "corpora/tokenized/*.csv",
    ]

    @staticmethod
    def sort_families_folders(folders: Iterable[Path]) -> List[Path]:
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
                group_words_dict = load_group_words(group, workspace)

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
    header = ["id", "filename", "voice", "frequency", "word", "phones", "length", "correct"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator=",", header=self.header)


class BuildZeroSpeechTestSetsTask(BaseTask, CorporaTaskMixin):
    requires = [
        "corpora/wuggy_pairs/*",
        "synth/audio/phonetic/",
    ]

    def __init__(self,
                 output_folder : Path,
                 real_word_synth: Literal["text", "phonetic"],
                 for_corpus: Optional[int] = None):
        super().__init__()
        self.real_word_synth = real_word_synth
        self.for_corpus = for_corpus
        self.output_folder  = output_folder

    def build_frequencies_csv(self,
                              zr_corpus_folder: Path,
                              corpus_id: int,
                              workspace: Workspace):
        family_folder = workspace.datasets / Path(f"families/family_{corpus_id}")
        corpus_csv = TokenizedWordsCSV(workspace.corpora / Path(f"tokenized/corpus_{corpus_id}.csv"))
        corpus_words = set(corpus_csv.to_dict().keys())

        group_paths = sorted(family_folder.iterdir(), key=lambda p: int(p.stem.split("_")[1]))
        freqs_df = pandas.DataFrame()
        for group_filepath in tqdm(group_paths):

            with open(group_filepath) as group_file:
                file_ids = {f_id for f_id in group_file.read().split("\n") if f_id}
            group_words_freqs = load_group_words(file_ids, workspace)

            group_words_freqs = SortedDict({word: freq
                                            for word, freq in group_words_freqs.items()
                                            if word in corpus_words})
            if freqs_df.index.empty:
                freqs_df.index = list(group_words_freqs.keys())

            freqs_df[group_filepath.name] = list(group_words_freqs.values())

        freqs_df.to_csv(zr_corpus_folder / Path("frequencies.csv"),
                        sep=",")

    def build_zr_testset(self,
                         wuggy_pairs_path: Path,
                         zr_corpus_folder: Path,
                         workspace: Workspace):
        zr_audio_folder = zr_corpus_folder / Path("ogg")
        zr_audio_folder.mkdir(parents=True, exist_ok=True)
        zr_csv_path = zr_corpus_folder / Path("gold.csv")

        phonetic_synth_folder = workspace.synth / Path("audio/phonetic/")
        text_synth_folder = workspace.synth / Path("audio/text/")
        voices = [p.name for p in phonetic_synth_folder.iterdir()]

        corpus_wuggy_pairs_csv = CandidatesPairCSV(wuggy_pairs_path)
        zr_corpus_csv = ZeroSpeechCSV(zr_csv_path)

        pbar = tqdm(total=corpus_wuggy_pairs_csv.lines_count * len(voices))

        with zr_corpus_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for pair_id, (word, phonetic, fake_phonetic) in enumerate(corpus_wuggy_pairs_csv):
                if self.real_word_synth == "text":
                    word_filename = word
                else:
                    word_filename = phonetic.replace(" ", "_")
                fake_word_filename = fake_phonetic.replace(" ", "_")

                for voice in voices:
                    # writing pair entries in the "gold" csv file
                    dict_writer.writerow({
                        "id": pair_id + 1,
                        "filename": f"{word_filename}-{voice}",
                        "voice": voice,
                        "frequency": 0.0,
                        "word": word,
                        "phones": phonetic,
                        "length": len(phonetic.split(" ")),
                        "correct": 1
                    })
                    dict_writer.writerow({
                        "id": pair_id + 1,
                        "filename": f"{fake_word_filename}-{voice}",
                        "voice": voice,
                        "frequency": 0.0,
                        "word": None,
                        "phones": fake_phonetic,
                        "length": len(fake_phonetic.split(" ")),
                        "correct": 0
                    })

                    # copying audio files for the pair
                    if self.real_word_synth == "text":
                        word_audio_path = text_synth_folder / Path(f"{voice}/{word_filename}.ogg")
                    else:
                        word_audio_path = phonetic_synth_folder / Path(f"{voice}/{word_filename}.ogg")
                    word_cp_path = zr_audio_folder / Path(f"{word_filename}-{voice}.ogg")
                    fake_word_audio_path = phonetic_synth_folder / Path(f"{voice}/{fake_word_filename}.ogg")
                    fake_word_cp_path = zr_audio_folder / Path(f"{fake_word_filename}-{voice}.ogg")

                    if not word_cp_path.is_file():
                        copyfile(word_audio_path, word_cp_path)
                    if not fake_word_cp_path.is_file():
                        copyfile(fake_word_audio_path, fake_word_cp_path)

                    pbar.update()

    def run(self, workspace: Workspace):
        zr_folder = self.output_folder
        zr_folder.mkdir(parents=True, exist_ok=True)
        wuggy_pairs_folder = workspace.corpora / Path("wuggy_pairs")

        corpora = self.find_corpora(wuggy_pairs_folder)
        if self.for_corpus is not None:
            corpora = [(corpus_id, corpus_path)
                       for corpus_id, corpus_path in corpora
                       if corpus_id == self.for_corpus]

        for corpus_id, corpus_csv_path in corpora:
            corpus_zr_folder = zr_folder / Path(f"testset_{corpus_id}")
            logger.info(f"For corpus {corpus_id}")
            logger.info("Building corpus table and aggregating audio files")
            self.build_zr_testset(corpus_csv_path, corpus_zr_folder, workspace)
            logger.info("Building corpus word frequencies table")
            self.build_frequencies_csv(corpus_zr_folder, corpus_id, workspace)
