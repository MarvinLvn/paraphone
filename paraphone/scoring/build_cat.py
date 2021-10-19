""""
Builds category file for pairs of inputs -- to be used before balance_scores_with_categories.py

path_pairs : path to file of ARPA pairs of words.
            /!\ Those pairs should have been matched beforhand if
            using the Google synthesizer at the end of the process.
freq_path : file with set of reference words with frequency of appearance
"""

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('path_pairs', help='Path to the file with ARPA pairs of words/non-words')
    parser.add_argument('freq_path', help='Path to output matching words')
    parser.add_argument('output_path', help='Type of token that is being processed', type=str)
    args = parser.parse_args()

    f = open(args.freq_path, "r")
    i = open(args.path_pairs, "r")
    g = open(args.output_path, "w+")
    m = f.readlines()
    n = i.readlines()
    d = {}
    c = 0
    for elem in m:
        d[elem.split('\t')[0].replace('1', '0')] = elem.split('\t')[1].split('\n')[0]
    words = []
    for j in range(1, len(n)):
        words.append(n[j].split('\t')[0])
    s = set(words)
    g.write("Word" + '\t' + 'length_phone' + '\t' + 'freq' + '\n')
    for w in s:
        try:
            g.write(w + '\t' + str(len(w.split(' '))) + '\t' + str(d[w.replace('1', '0')]) + '\n')
        except KeyError:
            g.write(w + '\t' + str(len(w.split(' '))) + '\t' + str(1) + '\n')
            print(w)
            c += 1
    print('Unrecognized word : {} out of '.format(c) + str(len(s)))
    g.close()
    f.close()
    i.close()
