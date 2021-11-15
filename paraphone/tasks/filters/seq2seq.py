import logging
import shlex
from pathlib import Path
from typing import Iterable, Tuple, Dict, Set
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

from ...utils import logger
from ...workspace import Workspace

try:
    import tensorflow.app as tf_app
    from g2p_seq2seq.app import main as seq2seq_main
except ImportError:
    pass

from .base import FilteringTaskMixin, WordPair
from ..base import BaseTask


class Seq2SeqMixin:

    @staticmethod
    def write_P2G_input(words: Iterable[str], file_path: Path):
        with open(file_path, "w") as txt_file:
            for word in words:
                txt_file.write(f'_\t{word}\n')

    @staticmethod
    def write_G2P_input(words: Iterable[str], file_path: Path):
        with open(file_path, "w") as txt_file:
            for word in words:
                txt_file.write(f'{word}\n')

    @staticmethod
    def read_P2G_output(file_path: Path) -> Iterable[Tuple[str, str]]:
        with open(file_path) as txt_file:
            for line in txt_file.readlines():
                line = line.strip()
                *phonemes, word = line.split(" ")
                yield " ".join(phonemes), word

    @staticmethod
    def read_G2P_output(file_path: Path) -> Iterable[Tuple[str, str]]:
        with open(file_path) as txt_file:
            for line in txt_file.readlines():
                line = line.strip()
                word, *phonemes = line.split(" ")
                yield word, " ".join(phonemes)


class Seq2SeqTrainTask(BaseTask):
    pass


class P2GTrain(Seq2SeqTrainTask):
    creates = [
        "candidates_filtering/seq2seq/p2g_model/",
    ]


class G2PTrain(Seq2SeqTrainTask):
    creates = [
        "candidates_filtering/seq2seq/g2p_model/",
    ]


class Seq2SeqFilterTask(FilteringTaskMixin, Seq2SeqMixin):

    @classmethod
    def decode_P2G(cls, input_file: Path, output_file: Path, workspace: Workspace):
        p2g_model_dir = workspace.candidates_filtering / Path("seq2seq/p2g_model/")
        decoding_args = [
            "--decode", str(input_file),
            "--model_dir", str(p2g_model_dir),
            "--output", str(output_file),
            "--p2g"
        ]
        logger.info(f"Using model in folder {p2g_model_dir}")
        logger.debug(f"Decoding args : {shlex.join(decoding_args)}")
        tf_app.run(main=seq2seq_main, argv=decoding_args)

    @classmethod
    def decode_G2P(cls, input_file: Path, output_file: Path, workspace: Workspace):
        g2p_model_dir = workspace.candidates_filtering / Path("seq2seq/g2p_model/")
        decoding_args = [
            "--decode", str(input_file),
            "--model_dir", str(g2p_model_dir),
            "--output", str(output_file),
        ]
        logger.info(f"Using model in folder {g2p_model_dir}")
        logger.debug(f"Decoding args : {shlex.join(decoding_args)}")
        tf_app.run(main=seq2seq_main, argv=decoding_args)


class P2GWordsFilterTask(Seq2SeqFilterTask):
    """Filter out real words whose graphemized form (from the phonemic form)
     doesn't match with its original graphemic form"""
    requires = [
        "candidates_filtering/seq2seq/p2g_model/",
    ]
    step_name = "seq2seq-words"

    def __init__(self):
        super().__init__()
        self.valid_words: Set[str] = set()

    def keep_pair(self, word_pair: WordPair) -> bool:
        return word_pair.word in self.valid_words

    def run(self, workspace: Workspace):
        model_workbench = workspace.candidates_filtering / Path("seq2seq/p2g_files/")

        logger.info("Preparing data for P2G decoding...")
        previous_step_csv = self.previous_step_csv(workspace)
        # {word phonetic form -> word} mapping
        pho_word_mapping = {word_pho: word for word, word_pho, _ in previous_step_csv}
        p2g_input_path = model_workbench / Path("words_p2g_input.csv")
        self.write_P2G_input(pho_word_mapping.keys(),
                             p2g_input_path)
        p2g_output_path = model_workbench / Path("words_p2g_decoded.txt")

        logger.info("Decoding the phonemic form of words into graphemes using seq2seq...")
        self.decode_P2G(input_file=p2g_input_path,
                        output_file=p2g_output_path,
                        workspace=workspace)

        logging.info("Loading decoded words and using them to filter out word candidates")
        # only keeping words whose decoded graphemic form match the original one
        for phonetic, decoded_grapheme in self.read_P2G_output(p2g_output_path):
            if pho_word_mapping[phonetic] == decoded_grapheme:
                self.valid_words.add(pho_word_mapping[phonetic])

        self.filter(workspace)


class G2PtoP2GNonWordsFilterTask(Seq2SeqFilterTask):
    """Filter our fake words whose 'rephonemized' forms don't match with
    the original phonemic form"""
    requires = [
        "candidates_filtering/seq2seq/p2g_model/",
        "candidates_filtering/seq2seq/g2p_model/",
    ]
    step_name = "seq2seq-nonwords"

    def __init__(self):
        super().__init__()
        # nonword pho -> nonword rephonemized pho
        self.rephomized_nonwords: Dict[str, str] = dict()

    def keep_pair(self, word_pair: WordPair) -> bool:
        return word_pair.word_pho == self.rephomized_nonwords[word_pair.word_pho]

    def run(self, workspace: Workspace):
        model_workbench = workspace.candidates_filtering / Path("seq2seq/p2g2p_files/")

        logger.info("Preparing data for P2G decoding...")
        previous_step_csv = self.previous_step_csv(workspace)
        p2g_input_path = model_workbench / Path("nonwords_p2g_input.csv")
        self.write_P2G_input((nonword_pho for _, _, nonword_pho in previous_step_csv),
                             p2g_input_path)
        p2g_output_path = model_workbench / Path("nonwords_p2g_decoded.txt")

        logger.info("Decoding the phonemic form of nonwords into graphemes using seq2seq...")
        self.decode_P2G(input_file=p2g_input_path,
                        output_file=p2g_output_path,
                        workspace=workspace)

        logging.info("Loading decoded nonwords to prepare them for G2P decoding...")
        g2p_input_path = model_workbench / Path("nonwords_g2p_input.csv")

        decoded_p2g = list(self.read_P2G_output(p2g_output_path))
        _, decoded_graphemes = zip(*decoded_p2g)
        self.write_G2P_input(decoded_graphemes, g2p_input_path)

        logger.info("Decoding the graphemic form of nonwords back into phonemes using seq2seq...")
        g2p_output_path = model_workbench / Path("nonwords_g2p_decoded.csv")
        self.decode_G2P(input_file=g2p_input_path,
                        output_file=g2p_output_path,
                        workspace=workspace)
        decoded_g2p = self.read_G2P_output(g2p_output_path)
        for (nonword_pho, _), (_, rephonemized_nonword) in zip(decoded_p2g, decoded_g2p):
            self.rephomized_nonwords[nonword_pho] = rephonemized_nonword

        self.filter(workspace)
