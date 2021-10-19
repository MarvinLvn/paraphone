from collections import Counter


def bigrams_bound_dict(freq_dataset):
    fin = open(freq_dataset, "r")
    m = fin.readlines()
    liste = []
    for line in m:
        word = remove_accents(line.split('\t')[0])
        freq = int(line.split('\t')[1][:-1])
        wordy = (' _ ' + word + ' _') * freq
        chars = wordy.split(' ')
        for e in chars:
            liste.append(e)
    bigrams = zip(*[liste[i:] for i in range(2)])
    c_ch = Counter(liste)
    c_bg = Counter(bigrams)
    dictionary = {}
    for key in c_bg.keys():
        dictionary[key] = c_bg[key] / c_ch[key[0]]
    return dictionary


def bigrams_unbound_dict(freq_dataset):
    fin = open(freq_dataset, "r")
    m = fin.readlines()
    liste = []
    for line in m:
        word = remove_accents(line.split('\t')[0])
        freq = int(line.split('\t')[1][:-1])
        wordy = (' ' + word + ' ') * freq
        chars = wordy.split(' ')
        for e in chars:
            liste.append(e)
    bigrams = zip(*[liste[i:] for i in range(2)])
    c_ch = Counter(liste)
    c_bg = Counter(bigrams)
    dictionary = {}
    for key in c_bg.keys():
        dictionary[key] = c_bg[key] / c_ch[key[0]]
    return dictionary


def unigrams_unbound_dict(freq_dataset):
    fin = open(freq_dataset, "r")
    m = fin.readlines()
    liste = []
    for line in m:
        word = remove_accents(line.split('\t')[0])
        freq = int(line.split('\t')[1][:-1])
        wordy = (' ' + word + ' ') * freq
        chars = wordy.split(' ')
        for e in chars:
            liste.append(e)
    bigrams = zip(*[liste[i:] for i in range(1)])
    c_bg = Counter(bigrams)
    dictionary = {}
    for key in c_bg.keys():
        dictionary[key] = c_bg[key] / sum(c_bg.values())
    return dictionary


def unigrams_bound_dict(freq_dataset):
    fin = open(freq_dataset, "r")
    m = fin.readlines()
    liste = []
    for line in m:
        word = remove_accents(line.split('\t')[0])
        freq = int(line.split('\t')[1][:-1])
        wordy = (' _ ' + word + ' _') * freq
        chars = wordy.split(' ')
        for e in chars:
            liste.append(e)
    bigrams = zip(*[liste[i:] for i in range(1)])
    c_bg = Counter(bigrams)
    dictionary = {}
    for key in c_bg.keys():
        dictionary[key] = c_bg[key] / sum(c_bg.values())
    return dictionary


def remove_accents(ARPA):
    ra = ARPA.replace('1', '0').replace('2', '0')
    return ra