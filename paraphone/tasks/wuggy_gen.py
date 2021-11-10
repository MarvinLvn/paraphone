import random
import re
from pathlib import Path
from types import ModuleType
from typing import Set, Iterable, Tuple, List

from tqdm import tqdm
from wuggy_ng import Generator

from .base import BaseTask
from .syllabify import SyllabifiedWordsCSV
from .tokenize import TokenizedTextCSV
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
        all_tokens_csv = TokenizedTextCSV(workspace.tokenized / Path("all.csv"))
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


class WuggyGenerationTask(BaseTask):
    requires = [
        "wuggy/lexicon.csv",
        "phonemized/syllabic.csv",
    ]
    creates = [
        "wuggy/candidates.csv"
    ]
    wuggy_plugin: ModuleType
    pho_sub_re = re.compile(r":")
    dash_sub_re = re.compile(r"-")

    def __init__(self, num_candidates: int, high_overlap: bool, num_workers: int):
        super().__init__()
        self.num_candidates = num_candidates
        self.high_overlap = high_overlap
        self.num_workers = num_workers

    @classmethod
    def normalize_wuggy_syllabic(cls, wuggy_syllabic: str) -> str:
        normalized = cls.pho_sub_re.sub(" ", wuggy_syllabic)
        normalized = cls.dash_sub_re.sub("- ", normalized)
        normalized = normalized.strip()
        return normalized

    def set_wuggy_plugin_params(self, lexicon_path: Path, wuggy_plugin: ModuleType):
        wuggy_plugin.default_data = str(lexicon_path)
        wuggy_plugin.default_neighbor_lexicon = str(lexicon_path)
        wuggy_plugin.default_word_lexicon = str(lexicon_path)
        wuggy_plugin.default_lookup_lexicon = str(lexicon_path)

    def generate_candidates(self, word: str, wuggy_gen: Generator) -> Set[str]:
        nonword_candidates = set()
        wuggy_gen.set_reference_sequence(wuggy_gen.lookup(word))
        for i in range(1, self.num_candidates):
            wuggy_gen.set_frequency_filter(2 ** i, 2 ** i)
            wuggy_gen.set_attribute_filter('sequence_length')
            wuggy_gen.set_attribute_filter('segment_length')
            wuggy_gen.set_statistic('overlap_ratio')
            wuggy_gen.set_statistic('lexicality')
            wuggy_gen.set_output_mode('syllabic')

            # it's important not to use the cache because we want wuggy to always
            # generate the best matching nonword. The use of the cache would prevent
            # to generate multiple times the same nonword and thus lower the quality
            # the pairs. (and would cause performance issues on big sets of words)
            for sequence in wuggy_gen.generate(clear_cache=True):

                try:
                    sequence.encode('utf-8')
                except UnicodeEncodeError:
                    print('the matching nonword is non-ascii (bad wuggy)')
                    continue

                if wuggy_gen.statistics['lexicality'] != "N":
                    continue
                if sequence in nonword_candidates:
                    continue

                nonword_candidates.add(sequence)
                if len(nonword_candidates) >= self.num_candidates:
                    break

        return nonword_candidates

    def run(self, workspace: Workspace):
        # loading the syllabified lexicon as {word -> (pho, syll)} dict
        syllabified_lexicon = SyllabifiedWordsCSV(workspace.phonemized
                                                  / Path("syllabic.csv")).to_dict()

        # loading what's needed for wuggy, and instantiating wuggy generator
        random.seed(4577)  # setting seed for deterministic output
        lexicon_path = workspace.wuggy / Path("lexicon.csv")
        words_path = workspace.wuggy / Path("words.txt")
        self.set_wuggy_plugin_params(lexicon_path, self.wuggy_plugin)
        wuggy_gen = Generator()
        wuggy_gen.data_path = "."
        wuggy_gen.load(phonetic_fr_ipa, open(lexicon_path))
        wuggy_gen.load_word_lexicon(open(lexicon_path))
        wuggy_gen.load_neighbor_lexicon(open(lexicon_path))
        wuggy_gen.load_lookup_lexicon(open(lexicon_path))
        logger.debug(f"Generator output modes : {wuggy_gen.list_output_modes()}")

        # Here are the set of all legal words
        legal_words: Set[str] = set(wuggy_gen.lookup_lexicon.keys())
        with open(words_path) as words_file:
            words = {word for word in words_file.read().split("\n") if word}
        logger.debug("There are {:d} legal words.".format(len(words & legal_words)))

        # creating the candidates CSV and running the generator
        candidates_csv = FakeWordsCandidatesCSV(workspace.wuggy / Path("candidates.csv"))
        with candidates_csv.dict_writer as dict_writer:
            dict_writer.writeheader()
            for idx, word in enumerate(tqdm(legal_words)):
                word: str
                word_pho, word_syll = syllabified_lexicon[word]
                word_pho = " ".join(word_pho)
                word_syll = "-".join(" ".join(syll) for syll in word_syll)
                for fake_word_syll in self.generate_candidates(word, wuggy_gen):
                    fake_word_syll = self.normalize_wuggy_syllabic(fake_word_syll)
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
