import argparse
import importlib
import os
import sys
from os.path import exists

import pandas

sys.path.insert(0, "../")
from metaphone import MetaDict


class Add:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("add", help="add dict or list to database")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database ',
            required=True
        )
        parser.add_argument(
            "--inputs",
            type=str,
            nargs="+",
            help='add dictionnaries, lists, corpus to be append to MetaDict : format : 3 columns separated by comma : word, count, transcription (optional)'

        )
        parser.add_argument("--vowel", action="store_true", help="records vowels")
        parser.add_argument("--syllable", action="store_true", help="records syllables")
        parser.add_argument(
            "--sets",
            type=str,
            nargs="+",
            help='give a wordset/phoneset name for each dict and list given in input, must be in the same order and same lenght'

        )
        parser.add_argument(
            "--phoneset",
            action='store_true',
            help="provide phenset and not wordset in csv 'word,transcription'"
        )
        #         parser.add_argument("--output", type=str, choices=["phone", "coda", "onset", "vowel", "syllables", "transcription", "word"], nargs="+", required=True)
        parser.add_argument("--overwrite", action="store_true")
        return parser

    @classmethod
    def run(cls, args):

        if len(args.inputs) != len(args.sets):
            raise IndexError("inputs and wordsets must have the same lenght")

        if args.overwrite:
            if exists(args.input):
                os.remove(args.input)
        metadict = MetaDict(args.input, l_wordset=args.sets)
        if not args.phoneset:
            for i in range(len(args.inputs)):
                print("set", args.sets[i])
                df = pandas.read_csv(args.inputs[i])[["word", "count"]]
                try:
                    df["word"] = df["word"].str.lower().drop_duplicates()
                except AttributeError:
                    pass
                df.to_sql(args.sets[i], metadict.sqliteConnection, if_exists='replace', index=False)
        else:
            for i in range(len(args.inputs)):
                print(args.sets[i])
                df = pandas.read_csv(args.inputs[i])[["word", "transcription"]]
                try:
                    df["word"] = df["word"].str.lower().drop_duplicates()
                except AttributeError:
                    pass
                if args.syllable:
                    df.to_sql(args.sets[i] + "_syll", metadict.sqliteConnection, if_exists='replace', index=False)
                elif args.vowel:
                    df.to_sql(args.sets[i] + "_vowel", metadict.sqliteConnection, if_exists='replace', index=False)
                else:
                    df.to_sql(args.sets[i], metadict.sqliteConnection, if_exists='replace', index=False)


#             metadict.create('ALTER TABLE {} ADD PRIMARY KEY (word);'.format(args.wordsets[i]))
#         if args.input is not None:
#             metadict.read_wordset(args.input)

#         for i in range(len(args.inputs)):
#             print(args.wordsets[i])
#             for word, count, transcription in read(args.inputs[i], phonesets[i]):
#                 print(word)
#                 metadict.append(word, count, args.wordsets[i], transcription=transcription)
#                 metadict.word.d_words[word].set_count(count, args.wordsets[i])
#         if "word" in args.output:
#             metadict.write_word_in(args.write.split('.csv')[0]+".word.csv")


class Match:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("match", help="match according to build-in or made functions")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument(
            "--functions",
            type=str,
            nargs="+",
            required=True,
            help="choose path.to.file.function to match words",
            default="src.main.python.utiles.indict_fr"
        )
        parser.add_argument(
            "--kwargs",
            type=str,
            action="store",
            help="write a dict as {uun:[list,of union,dict], ude:[list, of intersection, corpus], occ:10}"
        )
        parser.add_argument(
            "--wordsets",
            type=str,
            nargs="+",
            help='give a wordset name for each dict and list given in input, must be in the same order and same lenght'

        )
        #         parser.add_argument("--output", type=str, choices=["phone", "coda", "onset", "vowel", "syllables", "transcription", "word"], nargs="+", required=True)
        #         parser.add_argument("--write", action="store", type=str, required=True)
        #         parser.add_argument("--write-match", action="store", type=bool)
        return parser

    @classmethod
    def run(cls, args):
        import json

        print(args.functions)
        functions = [getattr(importlib.import_module(".".join(module.split(".")[:-1]), package="Pronunciation"),
                             module.split(".")[-1]) for module in args.functions]
        print(args.kwargs)
        print(functions)
        kwargs = json.loads(args.kwargs)  # {x.split(",")[0]:x.split(",")[1] for x in args.kwargs}
        print(kwargs)
        metadict = MetaDict(args.input, l_wordset=[])
        #         metadict.read_wordset(args.input)
        metadict.reinit(args.wordsets)
        metadict.match(functions, **kwargs)


#         if "word" in args.output:
#              metadict.write_word_in(args.write.split('.csv')[0]+".word.csv")
#         if args.write_match:
#             m_token, m_type, t_total, t_type = metadict.write_match(args.write)
#             print("match,tokens,%token,type,%type,total_token,total_type")
#             print( "match",m_token,m_token/t_total, m_type,m_type/t_type , t_total,t_type, sep=",")
#             with open(output.split('.csv')[0]+".word.match.csv", "w") as f:
#                 [f.write("{},{}\n".format(i,j)) for i, j in l]

class Reinit:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("reinit", help="reinit match to 0")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        #         metadict.read_wordset(args.input)
        metadict.reinit(args.wordsets)


# class Phonemizer:
#     @classmethod
#     def add_parser(cls,subparser):
#         parser = subparser.add_parser("phonemizer", help="phonemize words in IPA with bootphon phonemizer")
#         parser.set_defaults(func=cls.run)
#         parser.add_argument(
#             "--input",
#             type=str,
#             action="store",
#             help='load words database',
#             required=True
#         )                              
#         parser.add_argument("--words", type=str, action='store', choices=["all", "matched"], help="phonemize all or matched words", required=True)
#         parser.add_argument("--language", type=str, action='store', help="choose language from espeak languages. examples : 'en-us', 'fr-fr'")
#         parser.add_argument("--separator", type=str, action="store", help="word_sep,syll_sep,phone_sep")
#         parser.add_argument("--njobs", type=int, action="store", help="number of jobs for computation")


#         parser.add_argument("--output", type=str, choices=["phone", "coda", "onset", "vowel", "syllables", "transcription", "word"], nargs="+", required=True)
#         parser.add_argument("--write", action="store", type=str, required=True)
#         return parser

#     @classmethod
#     def run(cls, args):
#         metadict = MetaDict(l_wordset=[])
#         metadict.read_wordset(args.input)
#         l_wd = metadict.word.d_word.keys()
#         if args.words == "matched":
#             l_wd = [wd for wd in l_wd if metadict.word.d_word[wd].other['dic']]
#         _ = self.phonemize(l_wd, args.language, args.njobs, args.separator)

#         if "word" in args.output:
#              metadict.write_word_in(args.write.split('.csv')[0]+".word.csv")
#         ## add write phone

class Folding:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("folding", help="folding phonetic transcription from phoneset to phoneset")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument("--drop", action="store_true", help="if specified overwrite the 'to' phoneset")
        parser.add_argument("--overwrite", action="store_true", help="if specified overwrite the 'to' phoneset")
        parser.add_argument("--syllable", action="store_true", help="records syllables")
        parser.add_argument("--vowel", action="store_true", help="records vowel")
        parser.add_argument("--onset", action="store_true", help="records onset")
        parser.add_argument("--match", action="store_true", help="records match")
        parser.add_argument("--from_phoneset", type=str, action="store", required=True)
        parser.add_argument("--to_phoneset", type=str, action="store", required=True)
        parser.add_argument("--file_folding", type=str, action="store", required=True,
                            help="a csv containing minimum 2 columns with correspoundances between phonesets")
        parser.add_argument(
            "--phonesets",
            type=str,
            nargs="+",
            help="as many phonesets than in folding file, in same order(minimum 2)"
        )
        return parser

    @classmethod
    def run(cls, args):

        #         d_fold = read_file_folding(args.file_folding, args.from_phoneset, args.to_phoneset, args.phonesets)
        metadict = MetaDict(args.input, l_wordset=[])
        df = pandas.read_csv(args.file_folding)[[args.from_phoneset + "_tofold", args.to_phoneset + "_tofold"]]

        if args.syllable:
            df.to_sql(args.from_phoneset + "_syll" + "_to_" + args.to_phoneset + "_syll", metadict.sqliteConnection,
                      if_exists='replace', index=True, index_label="id")
            metadict.fold(args.from_phoneset + "_syll", args.to_phoneset + "_syll", args.from_phoneset,
                          args.to_phoneset, args.from_phoneset + "_syll" + "_to_" + args.to_phoneset + "_syll",
                          args.overwrite, args.drop, args.match)
        elif args.onset:
            df.to_sql(args.from_phoneset + "_onset" + "_to_" + args.to_phoneset + "_onset", metadict.sqliteConnection,
                      if_exists='replace', index=True, index_label="id")
            metadict.fold(args.from_phoneset + "_onset", args.to_phoneset + "_onset", args.from_phoneset,
                          args.to_phoneset, args.from_phoneset + "_onset" + "_to_" + args.to_phoneset + "_onset",
                          args.overwrite, args.drop, args.match)

        elif args.vowel:
            df.to_sql(args.from_phoneset + "_vowel" + "_to_" + args.to_phoneset + "_vowel", metadict.sqliteConnection,
                      if_exists='replace', index=True, index_label="id")
            metadict.fold(args.from_phoneset + "_vowel", args.to_phoneset + "_vowel", args.from_phoneset,
                          args.to_phoneset, args.from_phoneset + "_vowel" + "_to_" + args.to_phoneset + "_vowel",
                          args.overwrite, args.drop, args.match)
        else:
            df.to_sql(args.from_phoneset + "_to_" + args.to_phoneset, metadict.sqliteConnection, if_exists='replace',
                      index=True, index_label="id")
            metadict.fold(args.from_phoneset, args.to_phoneset, args.from_phoneset, args.to_phoneset,
                          args.from_phoneset + "_to_" + args.to_phoneset, args.overwrite, args.drop, args.match)


class Seq2seq:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("seq2seq", help="create file for g2p/p2g ")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='write train/test set',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help="choose a phoneset",
            required=True
        )
        parser.add_argument("--train", action="store_true")
        parser.add_argument("--test", action="store_true")
        parser.add_argument("--p2g", action="store_true")
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        with open(args.output, "w") as f:
            if args.train:
                rows = metadict.select(
                    f"""select word, transcription from {args.phoneset} where transcription != 'NONE';""")
                for word, transcription in rows:
                    if args.p2g:
                        f.write(f"{transcription}\t{word}\n")
                    else:
                        f.write(f"{word}\t{transcription}\n")
            if not args.train and args.test:
                pass


class Replacement:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("replacement", help="replace phones inside a phoneset (real folding)")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help='phoneset where replace phones',
            required=True
        )
        parser.add_argument(
            "--file",
            type=str,
            action="store"
        )
        parser.add_argument("--vowel", action="store_true", help="records syllables")
        parser.add_argument("--syllable", action="store_true", help="records syllables")
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        df = pandas.read_csv(args.file)[["from" + args.phoneset, "to" + args.phoneset]]

        if args.syllable:
            df.to_sql("fromto" + args.phoneset + "_syll", metadict.sqliteConnection, if_exists='replace', index=True,
                      index_label="id")
            metadict.replacement(args.phoneset + "_syll", "from" + args.phoneset, "to" + args.phoneset,
                                 "fromto" + args.phoneset + "_syll")
        elif args.vowel:
            df.to_sql("fromto" + args.phoneset + "_vowel", metadict.sqliteConnection, if_exists='replace', index=True,
                      index_label="id")
            metadict.replacement(args.phoneset + "_vowel", "from" + args.phoneset, "to" + args.phoneset,
                                 "fromto" + args.phoneset + "_syll")
        else:

            df.to_sql("fromto" + args.phoneset, metadict.sqliteConnection, if_exists='replace', index=True,
                      index_label="id")

            metadict.replacement(args.phoneset, "from" + args.phoneset, "to" + args.phoneset, "fromto" + args.phoneset)


class VOC:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("voc", help="compute syllables compouds:  onset from syllables file")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )

        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            required=True,
            help="choose phoneset of vowels and syllables"
        )

        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        metadict.voc(args.phoneset)


class Get:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("got", help="reinit match to 0")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument(
            "--table",
            type=str,
            action="store",
            help='table ',
            required=True
        )
        parser.add_argument(
            "--select",
            type=str,
            action="store",
            nargs="+",
            help='columns',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='file to write in ',
            required=True
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        metadict.get(args.output, args.table, args.select)


class Wordseg:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("wordseg",
                                      help="call wordeg for syllabification of matched phonetic transcription")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help='syllabification on phoneset',
            required=True
        )
        parser.add_argument(
            "--remove_onset",
            type=str,
            action="store",
            nargs="+",
            help='remove onset in database for sylabifications'
        )
        parser.add_argument(
            "--add_onset",
            type=str,
            action="store",
            nargs="+",
            help='add new onsets not in database for sylabifications'
        )
        parser.add_argument(
            "--add_vowel",
            type=str,
            action="store",
            nargs="+",
            help='add new onsets not in database for sylabifications'
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        #         metadict.read_wordset(args.input)
        metadict.call_wordseg(args.phoneset, args.add_onset if args.add_onset is not None else [],
                              args.remove_onset if args.remove_onset is not None else [],
                              args.add_vowel if args.add_vowel is not None else [])


class MatchPairs:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("matchpairs", help="find which words have not non word")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load words database',
            required=True
        )
        parser.add_argument(
            "--testset",
            type=str,
            action="store",
            help='load words database of testset',
            required=True
        )
        parser.add_argument(
            "--pairs_file",
            type=str,
            action="store",
            help='file containing pairs words-nonwords',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='write words having not non-words',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help='phoneset',
            required=True
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        #         rows = metadict.select(f"""
        #             select m.word, m.transcription, cr0.count from {args.phoneset} INNER JOIN match on m.word=match.word INNER JOIN cr0 ON cr0.word=m.word where match.matching=0 and match.del=0

        #         """)
        #         d_let = {i[1]:i for i in rows}
        rows = metadict.select(f"""
            select m.word, m.transcription, cr0.count from {args.phoneset} m INNER JOIN match on m.word=match.word INNER JOIN cr0 ON cr0.word=m.word where match.del=0
        
        """)
        print(len(rows))
        d_kept = {i[0]: i for i in rows}
        metadict = MetaDict(args.testset, l_wordset=[])
        rows = metadict.select(f"""
            select m.word from corpus m 
        
        """)
        smp = set([i[0] for i in rows])
        print(len(smp))
        print(len(d_kept.keys()))
        d_let = dict(d_kept)
        for wd, _, _ in d_kept.values():
            if wd not in smp:
                del d_let[wd]
        d_kept = dict(d_let)
        print(len(d_kept.keys()))
        df = pandas.read_csv(args.pairs_file, sep="\t", index_col=0)
        d_let = dict(d_kept)
        for wd, tr, _ in d_kept.values():
            wd = wd.strip(" ")
            try:
                a = df.loc[tr]
                del d_let[wd]
            except KeyError:
                continue
        with open(args.output, "w") as f:
            [
                f.write(f"""{i[0]}\t{i[1]}\t{i[2]}\n""") for i in d_let.values()
            ]


#         df.to_sql(args.phoneset+"pairs", metadict.sqliteConnection, if_exists='replace', index=False)

# def wordseg(subparser):
#     pass
# def syllabify(subparser):
#     pass
# def syllables_compound(subparser):
#     pass
# def phonemize_sentences(subparser):
#     pass

set_commands = set()
set_commands.add(Add)
set_commands.add(Match)
set_commands.add(Reinit)
set_commands.add(Folding)
set_commands.add(Seq2seq)
set_commands.add(Replacement)
set_commands.add(Get)
set_commands.add(Wordseg)
set_commands.add(MatchPairs)
# set_commands.add(syllabify)
set_commands.add(VOC)


# set_commands.add(phonemize_sentences)
def app():
    parser = argparse.ArgumentParser(
        description='Welcome to MetaDict',
        prog="Metadict",
        allow_abbrev=False,
        add_help=True
        #         fromfile_prefix_chars='@',
    )

    #     parser.add_argument("action", type=str, help="positional arg must be in [{}]".format(",".join([i.__name__ for i in set_commands])))
    subparsers = parser.add_subparsers(
        metavar='<command>',
        help="positional arg must be in [{}]".format(
            ",".join([i.__name__.lower() for i in set_commands]),
            dest="command"
        )
    )

    for command in set_commands:
        command.add_parser(subparsers)

    #     args = parser.parse_arg
    args = parser.parse_args()
    args.func(args)


#     args.command(args)
#     parser.add_argument('--new_input', action='append', type=str, 
#                         help='add dictionnaries, lists, corpus to be append to MetaDict : format : 3 columns separated by comma : word, count, transcription (optional)')
#     parser.add_argument('--read', action='store', type=str)
#     parser.add_argument('--get_oov_ge_occs', '--gogo', action='append', type=int)
#     parser.add_argument('--match_input', action="append", type=str, help='choose the union of wordset to match with union of nont specified wordsets')
#     parser.add_argument('--match_freq', action="store", type=int, default=0)
#     parser.add_argument('--phonemizer', action='store', type=str, choices=['none', 'all', 'oot'], default="none")
#     parser.add_argument('--phonemize', action='store', type=str, help='from_phoneset and to_phoneset')
#     parser.add_argument('--phone', action='store', type=str, default="/p")
#     parser.add_argument('--syll', action='store', type=str, default="/s")
#     parser.add_argument('--word', action='store', type=str, default="/w")
#     parser.add_argument('--syll_compound', action='store', type=str, default="/c")
#     parser.add_argument('--syllabify_wordseg', action='store', type=str, help='path to file with a list of syllables and a second file with a list of vowels')
#     parser.add_argument('--syllabify', action='store', type=str, help='from_phoneset and to_phoneset')
#     parser.add_argument('--folding', action='store', type=bool, default=False)
#     parser.add_argument('--phoneset', '-p', action='append', type=str, default='none', help='as many phoneset than new_inputs')
#     parser.add_argument('--wordset', '-w', action='append', type=str, default='none', help='as many wordset than new_inputs')


if __name__ == '__main__':
    app()
