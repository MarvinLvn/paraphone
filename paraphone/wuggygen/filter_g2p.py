import re
import sys

if sys.argv[1] == "g2p":
    with open("../wuggydict/non-words.fwords.txt", "r") as f:
        txt = f.readlines()[1:]
    d = {}
    #     txt.remove("l\tw\n")
    #     txt=txt[:10000]
    for line in txt:
        wd, nwd = line[:-1].split("\t")
        if d.get(wd, None) is None:
            d[wd] = []
        d[wd].append(nwd)
    with open("../wuggydict/results.g2p", "r") as f:
        lines = f.readlines()
    #     print(len(txt), len(lines))
    assert len(txt) == len(lines)
    cnt_same = 0
    cnt_g2p = 0
    print("Word", "Match", sep="\t")
    for l, ll in zip(txt, lines):
        check = True
        word, nwd = l[:-1].split("\t")
        nwd = "".join([i for i in re.split(r"[:\-]", nwd) if i != ""])
        g2p = "".join(ll[:-1].split(" ")[1:])
        #         print(nwd, g2p)
        if d.get(nwd, None) is not None:
            cnt_same += 1
            check = False
        #             continue
        if nwd != g2p:
            cnt_g2p += 1
            check = False
        #             continue
        if check:
            print(word, nwd, sep="\t")
#     print(cnt_same, cnt_g2p)
