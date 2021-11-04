import logging
import random
from fractions import Fraction
from pathlib import Path
from time import time
from types import ModuleType

from wuggy_ng import Generator

from .base import BaseTask
from .syllabify import SyllabifiedWordsCSV
from .tokenize import TokenizedTextCSV
from ..utils import logger
from ..workspace import WorkspaceCSV, Workspace
from ..wuggy_plugins import phonetic_fr_ipa


class WuggyLexiconCSV(WorkspaceCSV):
    header = ["phonetic", "syllabic", "frequency"]

    def __init__(self, file_path: Path):
        super().__init__(file_path, separator="\t", header=self.header)


class WuggyPrepareTask(BaseTask):
    """Prepares a lexicon file for wuggy, containing :
    - the phonetic form (IPA) (sɛt)
    - the syllabic form (IPA) (a:-v:ɛ:)
    - the frequency by million of words"""
    requires = [
        "phonemized/syllabic.csv",
        "tokenized/all.csv"
    ]

    creates = [
        "wuggy/lexicon.csv"
        "wuggy/words.txt"
    ]

    def run(self, workspace: Workspace):
        logging.info("Computing total number of words in corpus")
        all_tokens_csv = TokenizedTextCSV(workspace.tokenized / Path("all.csv"))
        all_words = all_tokens_csv.to_dict()
        total_words_count = sum(all_words.values())
        syllabic_csv = SyllabifiedWordsCSV(workspace.phonemized / Path("syllabic.csv"))
        logging.info(f"Writing wuggy lexicon from {syllabic_csv.file_path}")

        wuggy_lexicon_csv = WuggyLexiconCSV(workspace.wuggy / Path("lexicon.csv"))
        wuggy_words_path = workspace.wuggy / Path("words.txt")
        with wuggy_lexicon_csv.dict_writer as lexicon_writer, \
                open(wuggy_words_path, "w") as words_file:
            # NOTE: we're not writing the header on purpose!

            for word, phonetic, syllabic in syllabic_csv:
                # frequency is per million
                word_frequency = all_words[word] / total_words_count * 1_000_000
                # all phonemes have to be attached
                phonetic = phonetic.replace(" ", "")
                # replacing " " phonemes boundaries with ":"
                syllabic = syllabic.replace(" ", ":")
                lexicon_writer.writerow({
                    "phonetic": phonetic,
                    "syllabic": syllabic,
                    "frequency": word_frequency
                })
                words_file.write(phonetic + "\n")



class WuggyGenerationTask(BaseTask):
    requires = [
        "wuggy/lexicon.csv"
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
        random.seed(4577)
        lexicon_path = workspace.wuggy / Path("lexicon.csv")
        wuggy_gen = Generator()
        wuggy_gen.load(phonetic_fr_ipa, open(lexicon_path))
        wuggy_gen.load_word_lexicon(open(lexicon_path))
        wuggy_gen.load_neighbor_lexicon(open(lexicon_path))
        wuggy_gen.load_lookup_lexicon(open(lexicon_path))
        logger.debug(f"Generator output modes : {wuggy_gen.list_output_modes()}")

        # Here are the set of all legal words
        legal_words = wuggy_gen.lookup_lexicon

        logger.debug("There are {:d} legal words.".format(len(words.intersection(legal_words))))
        
        for idx, word in enumerate(words):
            j = 0
            if word not in legal_words:
                continue

            nonword_candidates = set()
            wuggy_gen.set_reference_sequence(wuggy_gen.lookup(word))
            for i in range(1, 10):
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
                    #if time() - word_start_time > 50:
                    #    break

                    try:
                        sequence.encode('utf-8')
                    except UnicodeEncodeError:
                        print('the matching nonword is non-ascii (bad wuggy)')
                        continue
                    match = False

                    if (sequence != word
                            and wuggy_gen.statistics['lexicality'] == "N"
                            and sequence not in nonword_candidates):
                        match = True

                    if match:
                        line = [word, sequence]
                        nonword_candidates.add(sequence)
                        lines.append('\t'.join(line))
                        j = j + 1
                        if j >= self.num_candidates:
                            break

                if (j >= ncandidates) or (time() - word_start_time > 50):
                    break



class WuggyGenerationFrTask(WuggyGenerationTask):
    pass


class WuggyGenerationEnTask(WuggyGenerationTask):
    pass
