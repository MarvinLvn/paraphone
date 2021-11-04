from wuggy_ng.plugins.subsyllabic_common import *

from . import orth_fr as language

public_name = 'Phonetic French (IPA)'
hidden_sequence = False


def transform(input_sequence, frequency=1):
    return pre_transform(input_sequence, frequency=frequency, language=language)
