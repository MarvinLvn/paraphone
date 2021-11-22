import multiprocessing
import random
import re
from pathlib import Path
from types import ModuleType
from typing import Set, Iterable, Tuple, List, Optional

import tqdm
from tqdm import tqdm
from wuggy_ng import Generator

from .base import BaseTask
from .syllabify import SyllabifiedWordsCSV
from .tokenize import TokenizedWordsCSV
from ..utils import logger, Phoneme, Syllable, parse_syllabic
from ..workspace import WorkspaceCSV, Workspace
from ..wuggy_plugins import phonetic_fr_ipa, phonetic_en_ipa


# TODO: make it per subcorpus
class WuggyLexiconCSV(WorkspaceCSV):
    header = ["word", "syllabic", "frequency"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)


class FakeWordsCandidatesCSV(WorkspaceCSV):
    header = ["word", "phonetic", "syllabic", "fake-phonetic", "fake-syllabic"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)

    def __iter__(self) -> Iterable[Tuple[str,
                                         List[Phoneme], List[Syllable],
                                         List[Phoneme], List[Syllable]]]:
        with self.dict_reader as dict_reader:
            for row in dict_reader:
                yield (row["word"],
                       row["phonetic"].split(" "),
                       parse_syllabic(row["syllabic"]),
                       row["fake-phonetic"].split(" "),
                       parse_syllabic(row["fake-syllabic"]))


class WuggyPrepareTask(BaseTask):
    """Prepares a lexicon file for wuggy, containing :
    - the word form (avec)
    - the syllabic form (IPA) (a:-v:ɛ:k:)
    - the frequency by million of words"""
    requires = [
        "phonemized/syllabic.csv",
        "datasets/tokenized/all.csv"
    ]

    creates = [
        "wuggy/lexicon.csv",
        "wuggy/words.txt"
    ]

    def run(self, workspace: Workspace):
        workspace.wuggy.mkdir(parents=True, exist_ok=True)
        logger.info("Computing total number of words in corpus")
        all_tokens_csv = TokenizedWordsCSV(workspace.tokenized / Path("all.csv"))
        all_words = all_tokens_csv.to_dict()
        total_words_count = sum(all_words.values())
        syllabic_csv = SyllabifiedWordsCSV(workspace.phonemized / Path("syllabic.csv"))
        logger.info(f"Writing wuggy lexicon from {syllabic_csv.file_path}")

        wuggy_lexicon_csv = WuggyLexiconCSV(workspace.wuggy / Path("lexicon.csv"))
        wuggy_words_path = workspace.wuggy / Path("words.txt")
        with wuggy_lexicon_csv.dict_writer as lexicon_writer, \
                open(wuggy_words_path, "w") as words_file:
            # NOTE: we're not writing the header on purpose!
            # it's not needed by wuggy

            for word, phonetic, syllabic in syllabic_csv:
                # frequency is per million
                word_frequency = all_words[word] / total_words_count * 1_000_000
                # joining phoneme boudaries with ":" and syll boundaries with "-"
                syllabic_wuggy_repr = "-".join(
                    "".join([pho + ":" for pho in syll if pho]) for syll in syllabic
                )

                lexicon_writer.writerow({
                    "word": word,
                    "syllabic": syllabic_wuggy_repr,
                    "frequency": word_frequency
                })
                words_file.write(word + "\n")


class WuggyGenerator:
    pho_sub_re = re.compile(r":")
    dash_sub_re = re.compile(r"-")

    def __init__(self, lexicon_path: Path, wuggy_plugin: ModuleType,
                 num_candidates: int):
        self.set_wuggy_plugin_params(lexicon_path, wuggy_plugin)
        self.gen = Generator()
        self.gen.data_path = "."
        self.gen.load(wuggy_plugin, open(lexicon_path))
        self.gen.load_word_lexicon(open(lexicon_path))
        self.gen.load_neighbor_lexicon(open(lexicon_path))
        self.gen.load_lookup_lexicon(open(lexicon_path))
        self.num_candidates = num_candidates

    def set_wuggy_plugin_params(self, lexicon_path: Path, wuggy_plugin: ModuleType):
        wuggy_plugin.default_data = str(lexicon_path)
        wuggy_plugin.default_neighbor_lexicon = str(lexicon_path)
        wuggy_plugin.default_word_lexicon = str(lexicon_path)
        wuggy_plugin.default_lookup_lexicon = str(lexicon_path)

    @classmethod
    def normalize_wuggy_syllabic(cls, wuggy_syllabic: str) -> str:
        normalized = cls.pho_sub_re.sub(" ", wuggy_syllabic)
        normalized = cls.dash_sub_re.sub("- ", normalized)
        normalized = normalized.strip()
        return normalized

    def generate_candidates(self, word: str) -> Optional[Set[str]]:
        if word not in self.gen.lookup_lexicon:
            return

        nonword_candidates = set()
        self.gen.set_reference_sequence(self.gen.lookup(word))
        for i in range(1, self.num_candidates):
            self.gen.set_frequency_filter(2 ** i, 2 ** i)
            self.gen.set_attribute_filter('sequence_length')
            self.gen.set_attribute_filter('segment_length')
            self.gen.set_statistic('overlap_ratio')
            self.gen.set_statistic('lexicality')
            self.gen.set_output_mode('syllabic')

            # it's important not to use the cache because we want wuggy to always
            # generate the best matching nonword. The use of the cache would prevent
            # to generate multiple times the same nonword and thus lower the quality
            # the pairs. (and would cause performance issues on big sets of words)
            for sequence in self.gen.generate(clear_cache=True):

                # TODO: maybe think about removing this block
                try:
                    sequence.encode('utf-8')
                except UnicodeEncodeError:
                    print('the matching nonword is non-ascii (bad wuggy)')
                    continue

                if self.gen.statistics['lexicality'] != "N":
                    continue
                if sequence in nonword_candidates:
                    continue

                nonword_candidates.add(sequence)
                if len(nonword_candidates) >= self.num_candidates:
                    break

        return {self.normalize_wuggy_syllabic(word)
                for word in nonword_candidates}


def wuggygen_initializer(lexicon_path: Path,
                         wuggy_plugin: ModuleType,
                         num_candidates: int):
    logger.info(f"Initializing wuggy generator for process {multiprocessing.current_process().name}")
    global wuggy_generator
    random.seed(4577)  # setting seed for deterministic output
    wuggy_generator = WuggyGenerator(lexicon_path, wuggy_plugin, num_candidates)


def wuggygen_runner(word: str) -> Tuple[str, Set[str]]:
    return word, wuggy_generator.generate_candidates(word)


class WuggyGenerationTask(BaseTask):
    requires = [
        "wuggy/lexicon.csv",
        "phonemized/syllabic.csv",
    ]
    creates = [
        "wuggy/candidates.csv"
    ]
    wuggy_plugin: ModuleType

    def __init__(self, num_candidates: int, high_overlap: bool, num_workers: int):
        super().__init__()
        self.num_candidates = num_candidates
        self.high_overlap = high_overlap
        self.num_workers = num_workers

    def run(self, workspace: Workspace):
        # loading the syllabified lexicon as {word -> (pho, syll)} dict
        syllabified_lexicon = SyllabifiedWordsCSV(workspace.phonemized
                                                  / Path("syllabic.csv")).to_dict()

        # loading what's needed for wuggy, and instantiating wuggy generator
        lexicon_path = workspace.wuggy / Path("lexicon.csv")
        words_path = workspace.wuggy / Path("words.txt")
        wuggy_gen = WuggyGenerator(lexicon_path, self.wuggy_plugin, self.num_candidates)

        # Here are the set of all legal words
        legal_words: Set[str] = set(wuggy_gen.gen.lookup_lexicon.keys())
        with open(words_path) as words_file:
            words = {word for word in words_file.read().split("\n") if word}
        logger.info(f"There are {len(legal_words)} legal words (out of {len(words)} words).")

        # creating the candidates CSV and running the generator
        candidates_csv = FakeWordsCandidatesCSV(workspace.wuggy / Path("candidates.csv"))
        pool = multiprocessing.Pool(processes=self.num_workers,
                                    initializer=wuggygen_initializer,
                                    initargs=(lexicon_path,
                                              self.wuggy_plugin,
                                              self.num_candidates),
                                    )
        with candidates_csv.dict_writer as dict_writer, pool:
            dict_writer.writeheader()
            # map_args = ((word, wuggy_gen) for word in legal_words)
            pool_map = pool.imap_unordered(wuggygen_runner, legal_words,
                                           chunksize=2 ** 7)
            for word, fake_words in tqdm(pool_map, total=len(legal_words)):
                if fake_words is None:
                    pass
                word: str
                fake_words: Set[str]
                word_pho, word_syll = syllabified_lexicon[word]
                word_pho = " ".join(word_pho)
                word_syll = "-".join(" ".join(syll) for syll in word_syll)
                for fake_word_syll in fake_words:
                    fake_word_pho = fake_word_syll.replace("- ", "")
                    dict_writer.writerow({
                        "word": word,
                        "phonetic": word_pho,
                        "syllabic": word_syll,
                        "fake-phonetic": fake_word_pho,
                        "fake-syllabic": fake_word_syll
                    })


class WuggyGenerationFrTask(WuggyGenerationTask):
    wuggy_plugin = phonetic_fr_ipa


class WuggyGenerationEnTask(WuggyGenerationTask):
    wuggy_plugin = phonetic_en_ipa
