import argparse
import sys

sys.path.insert(0, "../")
from Pronunciation import MetaDict
import re


class WuggyDict:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("wdict", help="generate wuggy dictionnary")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load database',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='where write wuggy dict',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help="which transcription",
            required=True
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        query = f"""select s.transcription, s.syllables, c.count 
        from {args.phoneset}_syllabify s INNER JOIN match m ON m.word=s.word  INNER JOIN cr0 c ON s.word=c.word order by c.count desc 
            """
        rows = metadict.select(query)
        total = sum([i[2] for i in rows])
        query = f"""select sum(count) from cr0 INNER JOIN match on cr0.word=match.word where matching=1 and del=0"""
        total = metadict.select(query)[0][0]
        #         print(total)
        with open(args.output, "w") as f:
            for trans, syll, count in rows:
                #                 print( trans, count)
                trans = "".join(re.split(r"[ ;]", trans))
                syll = ":".join(re.split(r"[;]", syll))
                syll = "-".join(re.split(r"[_]", syll))
                count = str(1000000 * (count / total))
                if len([i for i in "".join(re.split(r"[:-]", syll)) if i != ""]) == 0:
                    continue
                f.write(f"{trans}\t{syll}\t{count}\n")


class WuggyWords:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("words", help="generate wuggy dictionnary")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load database with transcriptions',
            required=True
        )
        parser.add_argument(
            "--from_db",
            type=str,
            action="store",
            help='generate transcription from word contained in this database',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='where write wuggy dict',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help="which transcription",
            required=True
        )

        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        from_db = MetaDict(args.from_db, l_wordset=[])
        query = f"""
            select corpus.word from corpus inner join match on corpus.word = match.word and match.del=0 and match.matching=1
        """
        print(query)
        rows = from_db.select(query)
        rows = tuple([i[0] for i in rows])
        query = f"""select DISTINCT word, transcription from {args.phoneset} where word in {rows} and transcription != 'NONE' limit 500"""
        rows = metadict.select(query)
        #         rows = ["".join(i[0].split(" ")) for i in rows]
        with open(args.output, "w") as f:
            [f.write(f"{''.join(i[1].split(' ')).strip(' ')}\n") for i in rows if len("".join(i[1].split(" "))) > 1]
        with open(args.output + "word", "w") as f:
            [f.write(f"{i[0]}\t{' '.join(i[1].split(' ')).strip(' ') + ' '}\n") for i in rows if
             len("".join(i[1].split(" "))) > 1]


class WuggyFreq:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("wfreq", help="generate words freq")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load database',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='where write wuggy dict',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help="which transcription",
            required=True
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        query = f"""select s.transcription, s.syllables, c.count 
        from {args.phoneset}_syllabify s INNER JOIN match m ON m.word=s.word  INNER JOIN cr0 c ON s.word=c.word order by c.count desc
            """
        rows = metadict.select(query)
        total = sum([i[2] for i in rows])
        query = f"""select sum(count) from cr0 INNER JOIN match on cr0.word=match.word where matching=1 and del=0"""
        total = metadict.select(query)[0][0]
        #         print(total)
        with open(args.output, "w") as f:
            for trans, syll, count in rows:
                #                 print( trans, count)
                trans = " ".join(re.split(r"[ ;]", trans))
                syll = " ".join(re.split(r"[;]", syll))

                syll = " ".join(re.split(r"[_]", syll))
                syll = " ".join([i for i in syll.split(" ") if i != ""])
                count = str(count)
                if len([i for i in "".join(re.split(r"[:-]", syll)) if i != ""]) == 0:
                    continue
                #                 syll = syll.strip(" ")
                #                 syll= syll.strip(" ̃")
                f.write(f"{syll}\t{count}\n")


class WuggyNonWords:
    @classmethod
    def add_parser(cls, subparser):
        parser = subparser.add_parser("nonwords", help="generate wuggy dictionnary")
        parser.set_defaults(func=cls.run)
        parser.add_argument(
            "--input",
            type=str,
            action="store",
            help='load database with transcriptions',
            required=True
        )
        parser.add_argument(
            "--from_db",
            type=str,
            action="store",
            help='generate transcription from word contained in this database',
            required=True
        )
        parser.add_argument(
            "--output",
            type=str,
            action="store",
            help='where write wuggy dict',
            required=True
        )
        parser.add_argument(
            "--phoneset",
            type=str,
            action="store",
            help="which transcription",
            required=True
        )
        parser.add_argument(
            "--nonwords",
            type=str,
            action="store",
            help="word/non-words file",
            required=True
        )
        return parser

    @classmethod
    def run(cls, args):
        metadict = MetaDict(args.input, l_wordset=[])
        from_db = MetaDict(args.from_db, l_wordset=[])
        query = f"""
            select corpus.word from corpus inner join match on corpus.word = match.word and match.del=0 and match.matching=1
        """
        print(query)
        rows = from_db.select(query)
        rows = tuple([i[0] for i in rows])
        query = f"""select DISTINCT transcription from {args.phoneset} where word in {rows} and transcription != 'NONE' """
        rows = metadict.select(query)
        rows = [" ".join(i[0].split(" ")) for i in rows]
        s_wd = set()
        [s_wd.add(" ".join(re.split(r"[ ;]", i))) for i in rows]
        with open(args.nonwords, "r") as f:
            lines = f.readlines()[1:]

        with open(args.output, "w") as f:
            f.write("Word\tMatch\n")
            for line in lines:
                wd, _ = line[:-1].split("\t")
                if wd in s_wd:
                    f.write(line)


set_commands = set()
set_commands.add(WuggyDict)
set_commands.add(WuggyFreq)
set_commands.add(WuggyWords)
set_commands.add(WuggyNonWords)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Welcome to Prewuggy',
        prog="prewuggy",
        allow_abbrev=False,
        add_help=True
        #         fromfile_prefix_chars='@',
    )
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
