from wuggy_ng.plugins.subsyllabic_common import *

from . import orth_fr as language

public_name = 'Phonetic French (IPA)'
default_data = None
default_neighbor_lexicon = None
default_word_lexicon = None
default_lookup_lexicon = None
hidden_sequence = False


def transform(input_sequence, frequency=1):
    return pre_transform(input_sequence, frequency=frequency, language=language)
