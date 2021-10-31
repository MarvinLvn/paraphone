import argparse

parser = argparse.ArgumentParser(
    description='Welcome to database parser',
    prog="database_parser",
    allow_abbrev=False,
    add_help=True
    #         fromfile_prefix_chars='@',
)
parser.add_argument("database", type=str, action="store", choices=['cmu', 'lexique', 'propres', 'morphynet'],
                    help='choose database to download')
parser.add_argument("--transcription", action="store_true")
parser.add_argument("--syllables", action="store_true")
### add (en) cmu
args = parser.parse_args()
if args.database == "cmu":
    import requests
    import re

    url = 'https://sourceforge.net/projects/cmusphinx/files/Acoustic%20and%20Language%20Models/French/fr.dict/download'
    r = requests.get(url)
    #     print(r.text)
    d = {}
    if args.transcription:
        print('word,transcription')
    for line in r.text.split("\n"):
        line = line.split(" ")
        word = line[0]
        trans = " ".join(line[1:])
        if re.match(r".*\([0-9]*\)$", word):
            #             print(word)
            word = re.sub(r"\([0-9]*\)", "", word)
        #             print(word)
        if d.get(word, None) is not None:
            continue
        #         if 'uy' in trans:
        #             trans = trans.replace('uy', 'uu')
        d[word] = trans
        if word == "":
            continue
        if args.transcription:
            print(word, trans, sep=",")
    if not args.transcription:
        print('word,count')
    for k, v in d.items():
        if k == '':
            continue

        if not args.transcription:
            print(k, 1, sep=",")

if args.database == "lexique":

    import pandas

    lex = pandas.read_csv('http://www.lexique.org/databases/Lexique383/Lexique383.tsv', sep='\t')
    #     print(lex.head())
    #     lex = lex['ortho', 'phon']
    d = {}
    if not args.syllables:
        if args.transcription:
            print('word,transcription')
        for index, row in lex.iterrows():
            word = row['ortho']
            phon = row['phon']
            if 'x' in phon:
                continue
            if d.get(word, None) is None:
                d[word] = []
            phon = " ".join(list(phon))
            d[word].append(phon)
            #         print(word, " ".join(list(phon)))
            if word == "":
                continue
            if args.transcription:
                print(word, phon, sep=",")
        if not args.transcription:
            print('word,count')
        for k, v in d.items():
            if k == '':
                continue

            if not args.transcription:
                print(k, 1, sep=",")

    else:
        #         for i in lex.columns:
        #             print(i)

        #         print(lex[["syll"]])
        s_syll = set()
        for index, row in lex.iterrows():
            syll = row['syll']
            word = row['ortho']
            if 'x' in syll:
                continue
            syll = set(syll.split("-"))
            s_syll = s_syll.union(syll)
        print("word,transcription")
        for i, s in enumerate(s_syll):
            print("word" + str(i), " ".join(s) + " ", sep=",")

if args.database == "propres":
    import pandas

    patronyme = pandas.read_csv('https://www.data.gouv.fr/fr/datasets/r/9ae80de2-a41e-4282-b9f8-61e6850ef449', sep=',',
                                header=0)
    prenom = pandas.read_csv('https://www.data.gouv.fr/fr/datasets/r/4b13bbf2-4185-4143-92d3-8ed5d990b0fa', sep=',',
                             header=0)
    s = set()
    for index, row in patronyme.iterrows():
        if "ENT" == row["patronyme"] or "HES" == row["patronyme"] or "*" in row["patronyme"]:
            continue
        s.add(row["patronyme"])
    for index, row in prenom.iterrows():
        if "ENT" == row["prenom"] or "HES" == row["prenom"] or "*" in row["prenom"]:
            continue
        s.add(row["prenom"])
    [print(i, 1, sep=",") for i in s]

if args.database == "morphynet":
    from phonemizer.phonemize import phonemize
    from phonemizer.separator import Separator
    import pandas

    inflex = pandas.read_csv('https://raw.githubusercontent.com/kbatsuren/MorphyNet/main/eng/eng.inflectional.v1.tsv',
                             sep='\t',
                             header=None,
                             index_col=False)
    deriv = pandas.read_csv('https://raw.githubusercontent.com/kbatsuren/MorphyNet/main/eng/eng.derivational.v1.tsv',
                            sep='\t',
                            header=None,
                            index_col=False)
    d_pos = {}
    d_aff = {}
    # get word with uniq pos
    ##inflex
    for row in inflex.itertuples():
        #         print(tuple(row))
        _, wd, mrphm, pos, suff = tuple(row)
        if isinstance(pos, float):
            continue
        if pos == "-":
            continue
        pos = pos.split("|")[0]
        if pos not in ['V', 'N', 'ADJ']:
            continue
        if pos == 'ADJ':
            pos = "J"
        if d_pos.get(mrphm, None) is None:
            d_pos[mrphm] = set()
        d_pos[mrphm].add(pos)

    ##deriv
    for row in deriv.itertuples():
        _, wd, mrphm, POSw, POSm, affix, position = tuple(row)
        if POSm not in ['V', 'N', 'J']:
            continue
        if d_pos.get(mrphm, None) is None:
            d_pos[mrphm] = set()
        d_pos[mrphm].add(POSm)

    d_mp = dict(d_pos)
    for k, v in d_mp.items():
        if len(v) != 1:
            del d_pos[k]

    ## get from uniq pos word, their affixes
    # inflex

    #     with open("../morphynet/morphynet.pos.csv", "w") as f:
    #         for wd, pos in d_pos.items():
    #             pos = list(pos)[0]
    #             f.write(f"{wd},{pos}\n")
    #     with open("../morphynet/morphynet.affixes.csv", "w") as f:
    #         for wd, aff in d_aff.items():
    #             for pref in aff["prefix"]:
    #                 f.write(f"{wd},{pref},p\n")
    #             for suff in aff["suffix"]:
    #                 f.write(f"{wd},{suff},s\n")
    # take from corpus words in databases

    d_count = {}
    total = 0
    with open("../wordsgroup/en_1/0.txt", "r") as f:
        lines = f.readlines()[1:]
    for line in lines:
        wd, count = line[:-1].split(",")

        total += float(count)
        if d_pos.get(wd, None) is None:
            continue
        d_count[wd] = float(count)

    d_mp = dict(d_pos)
    # take in database , words in corpus
    for wd in d_mp.keys():
        if d_count.get(wd, None) is None:
            try:
                del d_pos[wd]
            except:
                pass

    a = list(d_pos.keys())
    trs = phonemize(
        a,
        backend="espeak",
        language="en-us",
        separator=Separator(phone="", word=""),
        preserve_punctuation=False,
        punctuation_marks="-",
        njobs=20
    )
    d_tr = {}
    print(len(trs), len(a))
    assert len(trs) == len(a)
    for tr, wd in zip(trs, a):
        if d_tr.get(tr, None) is None:
            d_tr[tr] = set()
        d_tr[tr].add(wd)
    for tr, wds in d_tr.items():
        if len(wds) != 1:
            for wd in wds:
                try:
                    del d_pos[wd]
                except:
                    pass
    d_afrq = {}
    for row in inflex.itertuples():
        _, wd, mrphm, pos, suff = tuple(row)
        if suff == "-":  ## delete word without affixes
            try:
                del d_pos[mrphm]
            except KeyError:
                continue
            continue
        if isinstance(suff, float):
            continue
        if d_count.get(mrphm, None) is None:
            continue
        suff = suff.split("|")[-1]
        if d_pos.get(mrphm, None) is None:
            continue
        if d_aff.get(wd, None) is None:
            d_aff[wd] = {"prefix": set(), "suffix": set()}
        d_aff[wd]["suffix"].add(suff)
        if d_afrq.get(suff, None) is None:
            d_afrq[suff] = {"V": 0, "N": 0, "J": 0, "Vt": 0, "Nt": 0, "Jt": 0}
        d_afrq[suff][list(d_pos[mrphm])[0]] += d_count[mrphm]
        d_afrq[suff][list(d_pos[mrphm])[0] + "t"] += 1

    ## fait expres de prendre un affix/mot comme dnas morphy net pour les quadrupelts ou on n'ajoute qu'un suel affix a chaque fois
    ####ROOT OR MORPHEM POS uniq???? MORPHEM vu qu'on dois faire avec nltk aussi
    #################################
    # deriv
    for row in deriv.itertuples():
        _, wd, mrphm, POSw, POSm, affix, position = tuple(row)
        if d_pos.get(mrphm, None) is None:
            continue
        if d_count.get(mrphm, None) is None:
            continue
        if d_aff.get(wd, None) is None:
            d_aff[wd] = {"prefix": set(), "suffix": set()}
        d_aff[wd][position].add(affix)
        if d_afrq.get(affix, None) is None:
            d_afrq[affix] = {"V": 0, "N": 0, "J": 0, "Vt": 0, "Nt": 0, "Jt": 0}
        d_afrq[affix][list(d_pos[mrphm])[0]] += d_count[mrphm]
        d_afrq[affix][list(d_pos[mrphm])[0] + "t"] += 1

    with open("../morphynet/morphynet.pos.csv", "w") as f:
        f.write("word,pos,occ,freq\n")
        for wd, pos in d_pos.items():
            pos = list(pos)[0]
            f.write(f"{wd},{pos},{d_count[wd]},{d_count[wd] / total}\n")
    with open("../morphynet/morphynet.words-affixes.csv", "w") as f:
        f.write("word,affix,pos\n")
        for wd, aff in d_aff.items():
            for pref in aff["prefix"]:
                f.write(f"{wd},{pref},p\n")
            for suff in aff["suffix"]:
                f.write(f"{wd},{suff},s\n")

    with open("../morphynet/morphynet.affixes.csv", "w") as f:
        f.write(
            "affix,type-V,typef-V,type-N,typef-N,type-J,typef-J,token-V,tokenf-V,token-N,tokenf-N,token-J,tokenf-J\n")
        for k, v in d_afrq.items():
            string = "{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
                k,
                v["Vt"],
                v["Vt"] / len(list(d_count.keys())),
                v["Nt"],
                v["Nt"] / len(list(d_count.keys())),
                v["Jt"],
                v["Jt"] / len(list(d_count.keys())),
                v["V"],
                v["V"] / total,
                v["N"],
                v["N"] / total,
                v["J"],
                v["J"] / total,
            )
            f.write(string)
