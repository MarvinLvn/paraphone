import argparse

import numpy as np

from .utils import bigrams_bound_dict, bigrams_unbound_dict, unigrams_bound_dict, unigrams_unbound_dict, remove_accents

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('freqset_path', help='dictionary used as LM for nonword generation with Wuggy - CELEX or CMU')
    parser.add_argument('input_path', help='dictionary used as LM for nonword generation with Wuggy - CELEX or CMU')
    parser.add_argument('output_path', help='dictionary used as LM for nonword generation with Wuggy - CELEX or CMU')
    args = parser.parse_args()
    bg = bigrams_bound_dict(args.freqset_path)
    bgu = bigrams_unbound_dict(args.freqset_path)
    ug = unigrams_bound_dict(args.freqset_path)
    ugu = unigrams_unbound_dict(args.freqset_path)
    i = open(args.input_path, "r")
    o = open(args.output_path, "w+")
    pairs = i.readlines()[1:]
    dict_words = {}
    o.write(
        'Word' + '\t' + 'score_unigram_bound' + '\t' + 'score_unigram_unbound' + '\t' + 'score_bigram_bound' + '\t' + 'score_bigram_unbound' + '\n')
    liste = []
    for elt in pairs:
        word = elt.split('\t')[0]
        nw = elt.split('\t')[1][:-1]
        try:
            dict_words[word] = 'in'
            liste.append(nw)
        except KeyError:
            dict_words[word] = 'in'
            liste.append(word)
            liste.append(nw)
    for w in liste:
        wb = '_ ' + remove_accents(w) + ' _'
        wf = remove_accents(w)
        lw = wf.split(' ')
        lwb = wb.split(' ')
        prob_bg = 1
        prob_bgu = 1
        prob_ug = ug[(lwb[0],)]
        prob_ugu = ugu[(lw[0],)]
        for i in range(1, len(lw)):
            try:
                prob_bgu = prob_bgu * bgu[(lw[i - 1], lw[i])]
            except KeyError:
                prob_bgu = 0
            try:
                prob_ugu = prob_ugu * ugu[(lw[i],)]
            except KeyError:
                prob_ugu = 0
        for i in range(1, len(lwb)):
            try:
                prob_bg = prob_bg * bg[(lwb[i - 1], lwb[i])]
            except KeyError:
                prob_bg = 0
            try:
                prob_ug = prob_ug * ug[(lwb[i],)]
            except KeyError:
                prob_ug = 0
        o.write(
            w + '\t' + str(np.log(prob_ug)) + '\t' + str(np.log(prob_ugu)) + '\t' + str(np.log(prob_bg)) + '\t' + str(
                np.log(prob_bgu)) + '\n')
    o.close()
