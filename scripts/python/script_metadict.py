import sys

sys.path.insert(0, "../")
from metaphone import MetaDict
import argparse
from os.path import join


def match(input, output, lang, n_fam, n):
    """print statistics about matching words"""

    a = " {}.count "
    l_a = ",".join([a.format(i) for i in n])
    b = " {} ON cr0.word = {}.word "
    l_b = " INNER JOIN " + " INNER JOIN ".join([b.format(i, i) for i in n[1:]])
    if len(n) == 1:
        l_b = ""
    dic = MetaDict(input)
    d = " count{} INTEGER "
    l_d = ",".join([d.format(i) for i in n])
    dic.create("drop  table if exists corpus")
    query = f"""create table if not exists corpus (word TEXT, {l_d}) 
              """
    #     print(query)
    dic.create(query)

    query = f"""insert into corpus select cr0.word, {l_a} from cr0 {l_b}"""
    #     print(query)
    dic.create(query)
    e = " corpus.count{} "
    l_e = ",".join([e.format(i) for i in n])
    query = f"""select match.word, {l_e} from match INNER JOIN corpus on match.word = corpus.word where match.del=0"""
    rows = dic.select(query)
    #     print(query)
    #     print(query)
    t_rows = dic.select(query)
    t_col = [0 for i in range(len(n))]
    for row in t_rows:
        for i in range(len(row[1:])):
            t_col[i] += row[1:][i]
    t_total = sum([sum(x[1:]) for x in t_rows])
    t_type = len(t_rows)
    query = f"""select match.word, {l_e} from match INNER JOIN corpus on match.word = corpus.word where match.del=0 and matching = 1"""
    rows = dic.select(query)
    m_token = sum([sum(x[1:]) for x in rows])
    m_type = len(rows)
    print("CMU", m_token, m_token / t_total, m_type, m_type / t_type, t_total, t_type, sep=",")
    f_rows = []
    for i in range(len(rows)):
        f_rows.append([rows[i][0]])
        for j in range(len(rows[i][1:])):
            f_rows[-1].append(str(rows[i][1:][j] / t_col[j]))
    with open(join(output, f"match_{lang}_{str(n_fam)}.csv"), "w") as f:
        [
            f.write(
                "{},{}\n".format(
                    ",".join(list([str(x) for x in i])), ",".join(list([str(x) for x in j])))
            ) for i, j in zip(rows, f_rows)
        ]


def occ(input, output, lang, n_fam, n, occs=1000):
    """
    print statistics about words which have a occs threshold
    """

    a = " {}.count "
    l_a = ",".join([a.format(i) for i in n])
    b = " {} ON cr0.word = {}.word "
    l_b = " INNER JOIN " + " INNER JOIN ".join([b.format(i, i) for i in n[1:]])
    if len(n) == 1:
        l_b = ""
    dic = MetaDict(input)
    d = " count{} INTEGER "
    l_d = ",".join([d.format(i) for i in n])
    dic.create("drop  table if exists corpus")
    query = f"""create table if not exists corpus (word TEXT, {l_d}) 
              """
    #     print(query)
    dic.create(query)

    query = f"""insert into corpus select cr0.word, {l_a} from cr0  {l_b}"""
    #     print(query)
    dic.create(query)

    c = " corpus.count{} >= {} "
    l_c = " AND ".join([c.format(i, str(occs)) for i in n])
    dic = MetaDict(input)
    e = " corpus.count{} "
    l_e = ",".join([e.format(i) for i in n])
    query = f"""select match.word, {l_e} from match INNER JOIN corpus on match.word = corpus.word where match.del=0"""
    rows = dic.select(query)
    #     print(query)
    t_rows = dic.select(query)
    t_col = [0 for i in range(len(n))]
    for row in t_rows:
        for i in range(len(row[1:])):
            t_col[i] += row[1:][i]
    t_total = sum([sum(x[1:]) for x in t_rows])
    t_type = len(t_rows)
    rows = dic.select(
        f"""select match.word, {l_e} from match INNER JOIN corpus on match.word = corpus.word where match.del=0 and match.matching=0 AND {l_c}""")
    m_token = sum([sum(x[1:]) for x in rows])
    m_type = len(rows)
    print("^CMU; >= " + str(occs), m_token, m_token / t_total, m_type, m_type / t_type, t_total, t_type, sep=",")
    f_rows = []
    for i in range(len(rows)):
        f_rows.append([rows[i][0]])
        for j in range(len(rows[i][1:])):
            f_rows[-1].append(str(rows[i][1:][j] / t_col[j]))
    with open(join(output, f"occs{str(occs)}_{lang}_{str(n_fam)}.csv"), "w") as f:
        [
            f.write(
                "{},{}\n".format(
                    ",".join(list([str(x) for x in i])), ",".join(list([str(x) for x in j])))
            ) for i, j in zip(rows, f_rows)
        ]


if __name__ == "__main__":
    #     app()

    parser = argparse.ArgumentParser(
        description='Welcome to Tokenize',
        prog="Tokenize",
        allow_abbrev=False,
        add_help=True
        #         fromfile_prefix_chars='@',
    )
    parser.add_argument(
        "--input",
        help="database",
        action="store",
        type=str
    )
    parser.add_argument(
        "--output",
        help="folder to output in",
        action="store",
        type=str
    )
    parser.add_argument(
        "--n_fam",
        help="number of families",
        action="store",
        type=int
    )
    parser.add_argument(
        "--lang",
        help="en or fr or other",
        action="store",
        type=str
    )
    parser.add_argument(
        "--i",
        help="set of data",
        action="store",
        type=str,
        nargs="+"
    )

    args = parser.parse_args()
    print(f"operator,tokens,%token,type,%type,total_token,total_type")
    match(args.input, args.output, args.lang, args.n_fam, args.i)
    occ(args.input, args.output, args.lang, args.n_fam, args.i, 1)
#     occ(args.input, args.output, args.lang, args.n_fam, args.i, 5)
#     occ(args.input, args.output, args.lang, args.n_fam, args.i, 10)
#     occ(args.input, args.output, args.lang, args.n_fam, args.i, 50)
#     occ(args.input, args.output, args.lang, args.n_fam, args.i, 100)
#     occ(args.input, args.output, args.lang, args.n_fam, args.i, 500)
#     occ(args.input, args.output, args.lang, args.n_fam, args.i, 1000)
