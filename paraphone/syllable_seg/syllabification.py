"""Estimates syllable boundaries on a text using the maximal onset principle.

This algorithm fully syllabifies a text from a list of onsets and
vowels. Input text must be in orthographic form (with word separators
only) or in phonemized form (with both word and phone
separators). Output text has syllable separators added at estimated
syllable boundaries. For exemples of vowels and onsets files, see the
directory `wordseg/data/syllabification`.

"""

# Created by Lawrence Phillips & Lisa Pearl (2013), adapted by Alex
# Cristia (2015), converted from perl to python and integration in
# wordseg by Mathieu Bernard (2017). Credit is owed mainly to the
# original authors.


from typing import List, Set, Tuple

from .separator import Separator
from ..utils import null_logger


class UnknownSymbolError(RuntimeError):
    pass

class NoVowelError(RuntimeError):
    pass

class NoOnsetError(RuntimeError):
    pass

class Syllabifier:
    """Syllabify a text given in phonological or orthographic form

    Syllabification errors can occur when the onsets and/or vowels are
    not adapted to the input text (see the `tolerant` parameter).

    Parameters
    ----------
    onsets
        The list of valid onsets in the `text`
    vowels
        The list of vowels in the `text`
    separator : Separator, optional
        Token separation in the `text`
    log : logging.Logger, optional
        Where to send log messages

    Raises
    ------
    ValueError
        If `onsets` or `vowels` are empty or are not lists.

    """

    def __init__(self, onsets: List[str],
                 vowels: List[str],
                 separator: Separator = Separator(),
                 log=null_logger()):
        self.onsets: Set[Tuple[str]] = set(tuple(separator.tokenize(onset,
                                                                    level="phone"))
                                           for onset in onsets)
        self.vowels: Set[str] = set(vowels)
        self.separator: Separator = separator
        self.log = log

        # ensure onsets and vowels are not empty
        if not isinstance(vowels, list) or not len(vowels):
            raise ValueError('unvalid or empty vowels list')
        if not isinstance(onsets, list) or not len(onsets):
            raise ValueError('unvalid or empty onsets list')

        # concatenation of all chars in onsets and vowels (usefull to
        # detect any phone token during syllabification)
        self.symbols: Set[str] = set(self.vowels)
        for onset in self.onsets:
            self.symbols.update(set(onset))

    def syllabify(self, utterance: str, strip=False):
        """Parameters
        ----------
        utterance : str
            The input text to be syllabified, in valid phonological form
        strip : bool, optional
            When True, removes the syllable boundary at the end of words.

        Returns
        -------
        The text with estimated syllable boundaries added. If `tolerant`
        is True some utterances may be missing in the output.

        Raises
        ------
        ValueError
            If an utterance has not been correctly syllabified . If
            `separator.syllable` is found in the text, or if `onsets`
            or `vowels` are empty.

        Raises
        ------
        RuntimeError
            If the syllabification failed

        """
        # split the utterances into words
        words = self.separator.tokenize(utterance, level='word',
                                        keep_boundaries=True)

        # estimate syllables boundaries word per word, read them from
        # end to start
        output = ''
        for n, word in enumerate(words[::-1]):
            output_word = self._syllabify_word(word, strip)
            output_word = self.separator.phone.join(output_word)

            # concatenate the syllabified word to the output, do not
            # append a word separator at the end if stripped
            if strip and not self.separator.remove(output):
                output = output_word
            else:
                output = output_word + self.separator.word + output

        return output

    def _syllabify_word(self, word: str, strip: bool):
        """Return a single word with syllable boundaries added

        Auxiliary function to syllabify_utterance().

        Raises
        ------
        RuntimeError
            If the word has no vowel, contains an unknown symbol (not
            present in vowels or onsets) or if the syllabification
            failed.

        """
        word = self.separator.tokenize(word, level="phone")
        # ensure all the chars in word are defined in vowels or onsets
        unknown = self._unknown_char(word)
        if unknown:
            raise UnknownSymbolError(
                'unknown symbol "{}" in word "{}"'.format(unknown, word))

        # ensure the word contains at least a vowel
        if not self._has_vowels(word):
            raise NoVowelError('no vowel in word "{}"'.format(word))

        input_word = list(word)
        output_word: List[str] = []
        syllable: List[str] = []

        # read characters of the current word from end to start
        while word:
            char = word.pop()

            # append current char to current syllable - that will be
            # necessary regardless of whether it's a vowel or a coda
            syllable = [char] + syllable

            if char in self.vowels:
                word, syllable = self._build_onset(word, syllable)

                # add the syllable to words entry
                if strip and not output_word:
                    output_word = syllable
                else:
                    output_word = (syllable + [self.separator.syllable] + output_word)
                syllable = []

        # removing syllable separators for a sanity check on onsets
        output_no_syll = [symbol for symbol in output_word if symbol != self.separator.syllable]
        if input_word != output_no_syll:
            raise NoOnsetError('onset not found in "{}"'.format(input_word,
                                                                output_no_syll))

        return output_word

    def _build_onset(self, word: List[str], syllable: List[str]):
        try:
            prevchar = word[-1]
            if prevchar not in self.vowels:
                # if this char is a vowel and the previous one is not,
                # then we need to make the onset, start with nothing as
                # the onset
                onset = []

                # then we want to take one letter at a time and check
                # whether their concatenation makes a good onset
                while len(word) and tuple([word[-1]] + onset) in self.onsets:
                    onset = [word.pop()] + onset

                # we get here either because we've concatenated the
                # onset+rest or because there was no onset and the
                # preceding element is a vowel, so this is the end of the
                # syllable
                syllable = onset + syllable
        except IndexError:  # there is no previous char
            pass

        return word, syllable

    def _unknown_char(self, word):
        """Returns the unknown char if anyone if found, False otherwise"""
        for w in word:
            if w not in self.symbols:
                return w
        return False

    def _has_vowels(self, word):
        """True if the `word` contains any vowel, False otherwise"""
        return bool(self.vowels & set(word))
