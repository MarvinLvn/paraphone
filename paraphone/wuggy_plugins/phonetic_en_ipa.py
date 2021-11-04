from . import orth_en as language
from wuggy_ng.plugins.subsyllabic_common import *

public_name = 'Phonetic English (IPA)'
hidden_sequence = False

def transform(input_sequence, frequency=1):
    return pre_transform(input_sequence, frequency=frequency, language=language)
