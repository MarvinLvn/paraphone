l = []
import re
# iː
import sys

if sys.argv[1] == "word":
    #     d_wd = {}
    d_wug = {}
    with open('../wuggydict/words.txtword', "r") as f:
        lines = f.readlines()
    for line in lines:
        wd, tr = line[:-1].split('\t')
        if d_wug.get(tr, None) is None:
            d_wug[tr] = True
        else:
            continue
    with open("../wuggydict/test.word", "w") as f:
        for key in d_wug.keys():
            f.write(f"_\t{key}\n")
# 
if sys.argv[1] == "res-word":
    d_wd = {}
    d_tr = {}
    false = 0
    with open('../wuggydict/words.txtword', "r") as f:
        lines = f.readlines()
    for line in lines:
        wd, tr = line[:-1].split("\t")
        d_wd[wd] = tr
        d_tr["".join(tr.strip(' ').split(" "))] = wd
    with open("../wuggydict/results.word", 'r') as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if i % 2 == 0:
            continue
        tr = "".join(line[:-1].split(" ")[:-1])
        wd = line[:-1].split(" ")[-1]
        #         print(wd, tr)
        if d_tr[tr] != wd:
            print(d_tr[tr], wd, tr, sep="\t")

            del d_tr[tr]
            false += 1
    print(false, "word bad graphemized")
    with open("../wuggydict/non-words.txt", "r") as f:
        lines = f.readlines()[1:]
    with open("../wuggydict/non-words.fwords.txt", "w") as f:
        for line in lines:
            wd, nwd = line[:-1].split("\t")
            #             print(wd, nwd)
            if d_tr.get(wd.strip(" "), None) is None:
                continue
            f.write(line)
if sys.argv[1] == "test-p2g":

    d_wug = {}
    with open('../wuggydict/wuggydict.tsv', 'r') as f:
        lines = f.readlines()
    for line in lines:
        wd, sylls, _ = line[:-1].split('\t')
        d_wug[wd] = len(sylls.split("-"))
    with open("../wuggydict/non-words.fwords.txt", "r") as f:
        lines = f.readlines()[1:]
    false = 0
    with open("../wuggydict/test.p2g", "w") as f:

        for line in lines:
            wd, nwd = line[:-1].split("\t")
            if len(nwd.split("-")) != d_wug[wd]:
                print(line[:-1], "syll")
                false += 1
                continue
            nwd = " ".join([i for i in re.split(r"[:\-]", nwd) if i != ""])
            f.write(f"_\t{nwd}\n")
        print(false, "non-word bad syllabified")
#     with open("../folding/phone-folding.txt", "r") as f:
#         for line in f:
#             line = line[:-1].split(",")[-1]
# #             if len(line.split(":")) > 1:
# #                 continue
#             l.append(line)
#     with open("../wuggydict/non-words.txt", "r") as f:
#         txt = f.readlines()[1:]
#         ll=[]
#         for line in txt:
#             line=line[:-1].split("\t")[1]
#             i = 0
#             w = []
#             while i < len(line) -1:
#                 p = line[i:i+2]
#                 if p in l:
#                     w.append(p)
# #                     print(p)
#                     i+=2
#                 else:
#                     w.append(p[0])
# #                     print(p[0])
#                     i+=1

# #                 i += 2
#             if len(line)!= len("".join(w)):
#                 w +=[line[-1]]
# #             print("")
#             print( '_'," ".join(w),sep="\t")
# #         ll.append(" ".join(w))
if sys.argv[1] == "test-g2p":
    with open("../wuggydict/results.p2g", "r") as f:
        prec = ""
        for i, line in enumerate(f):
            if i % 2 == 0:
                continue
            #             if line[:-1] == "_ s" and  line[:-1] != prec:
            #                 prec = line[:-1]
            #                 continue
            #             if line[0] ==" ":
            #                 continue
            #             prec = ""
            print(line[:-1].split(" ")[-1])

if sys.argv[1] == "scoring":
    with open("../folding/phone-folding.txt", "r") as f:
        for line in f:
            line = line[:-1].split(",")[-1]
            #             if len(line.split(":")) > 1:
            #                 continue
            l.append(line)
    with open("../wuggydict/results.fg2p2g", "r") as f:
        txt = f.readlines()[1:]
        ll = []
        print("Word\tMatch")
        for lines in txt:
            line = lines[:-1].split("\t")[0]
            i = 0
            w = []
            while i < len(line) - 1:
                p = line[i:i + 2]
                if p in l:
                    w.append(p)
                    #                     print(p)
                    i += 2
                else:
                    w.append(p[0])
                    #                     print(p[0])
                    i += 1

            #                 i += 2
            if len(line) != len("".join(w)):
                w += [line[-1]]
            #             print("")
            line = lines[:-1].split("\t")[1]
            i = 0
            wn = []
            while i < len(line) - 1:
                p = line[i:i + 2]
                if p in l:
                    wn.append(p)
                    #                     print(p)
                    i += 2
                else:
                    wn.append(p[0])
                    #                     print(p[0])
                    i += 1

            #                 i += 2
            if len(line) != len("".join(wn)):
                wn += [line[-1]]
            #             print("")
            print(" ".join(w), " ".join(wn), sep="\t")
#         ll.append(" ".join(w))
