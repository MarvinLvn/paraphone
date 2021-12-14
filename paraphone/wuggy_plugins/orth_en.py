import re

single_vowels = ["ə",
                 "ʊ",
                 "ɛ",
                 "iː",
                 "ɔ",
                 "ɪ",
                 "aɪ",
                 "ɑ",
                 "æ",
                 "oʊ",
                 "eɪ",
                 "iə",
                 "aʊ",
                 "ɑː",
                 "uː",
                 "ɜː",
                 "ɔː",
                 "ɔɪ",
                 "ʌ"]

nucleuspattern = '%s' % (single_vowels)
oncpattern = re.compile('(.*?)(%s)(.*)' % nucleuspattern)
