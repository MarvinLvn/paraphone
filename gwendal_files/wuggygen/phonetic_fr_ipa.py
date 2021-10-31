from wuggy_ng.plugins.orth import fr as language
from wuggy_ng.plugins.subsyllabic_common import *

public_name = 'Phonetic French (IPA)'
default_data = '../wuggydict/wuggydict.tsv'
default_neighbor_lexicon = '../wuggydict/wuggydict.tsv'
default_word_lexicon = '../wuggydict/wuggydict.tsv'
default_lookup_lexicon = '../wuggydict/wuggydict.tsv'
hidden_sequence = False

def transform(input_sequence, frequency=1):
    return pre_transform(input_sequence, frequency=frequency, language=language)
