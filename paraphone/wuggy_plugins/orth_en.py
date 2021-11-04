import re

# TODO
single_vowels = ["u",
                 "ɛ̃",
                 "e",
                 "i",
                 "o",
                 "a",
                 "ɛ",
                 "ɔ̃",
                 "ɑ̃",
                 "o",
                 "œ",
                 "ø",
                 "ə",
                 "œ̃",
                 "y", ]
nucleuspattern = '%s' % (single_vowels)
oncpattern = re.compile('(.*?)(%s)(.*)' % nucleuspattern)
