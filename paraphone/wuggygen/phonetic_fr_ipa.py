# Phonetic English
import sys

sys.path.insert(0, "../")
public_name = 'Phonetic English (IPA)'
default_data = '../wuggydict/wuggydict.tsv'
default_neighbor_lexicon = '../wuggydict/wuggydict.tsv'
default_word_lexicon = '../wuggydict/wuggydict.tsv'
default_lookup_lexicon = '../wuggydict/wuggydict.tsv'
hidden_sequence = False
from subsyllabic_common import *
from GenerateWuggy import language


def transform(input_sequence, frequency=1):
    return pre_transform(input_sequence, frequency=frequency, language=language)
