"""
Extracts a dictionary from the corpus in input and generates a list of nonwords, matched over
segment length, total length and transition frequencies
/!\ specifically designed to work with the CELEX dictionary.
"""

import argparse
from contextlib import contextmanager
from fractions import *
from multiprocessing import Pool, current_process
from time import time

import phonetic_fr_ipa
from wuggy_ng import Generator


@contextmanager
def poolcontext(*args, **kwargs):
    pool = Pool(*args, **kwargs)
    yield pool
    pool.terminate()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    n = max(1, n)
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path', help='corpus from which a dictionary will be extracted')
    parser.add_argument('output_path', help='output file containing a table of words and matches')
    parser.add_argument('--num-workers', '-w', default=20, help='number of parallel workers (defaut: 20)', type=int)
    parser.add_argument('--num-candidates', '-n', default=10, help='maximum number of nonword candidates per word \
                            (for some words, the number of candidates can be less than NUM_CANDIDATES) (default: 10)',
                        type=int)
    parser.add_argument('--max-time-per-word', '-t', default=10,
                        help='maximum time allowed for generating nonwords for one word (in seconds) (default: 10)',
                        type=float)
    parser.add_argument('--high-overlap', default=False,
                        help='if set to True, only allows overlap rate of the form (n-1)/n (attention: if True, much more slow!) (default: False)',
                        type=bool)
    args = parser.parse_args()

    # Parameters of the generation
    n_workers = args.num_workers
    ncandidates = args.num_candidates  # (maximum) number of nonword candidates per word (attention, for some words, the number of candidates can be less than ncandidates).
    max_time_per_word = args.max_time_per_word  # maximum time allowed for generating nonwords for one word (in seconds)
    high_overlap = args.high_overlap  # if this is True, only allows overlap rate of the form (n-1)/n
    print("Running parameters:")
    print("\t - NUM_WORKERS: {}".format(n_workers))
    print("\t - NUM_CANDIDATES: {}".format(ncandidates))
    print("\t - MAX_TIME_PER_WORD: {}".format(max_time_per_word))
    print("\t - HIGH_OVERLAP: {}".format(high_overlap))

    import io

    # dictionary of the corpus
    print("Opening input file from " + args.input_path)
    with io.open(args.input_path, "r", encoding="utf-8") as f:
        text = f.readlines()
        # words = set(re.findall(r'([\S]+)', text))
        words = set([elt[:-1] for elt in text])
    print("{:d} words found!".format(len(words)))

    print("Loading the Generator...")
    g = Generator()
    g.data_path = ('../wuggydict')
    g.load(phonetic_fr_ipa, io.open("../wuggydict/wuggydict.tsv", mode="r", encoding="utf-8"))
    g.load_word_lexicon(io.open("../wuggydict/wuggydict.tsv", mode="r", encoding="utf-8"))
    g.load_neighbor_lexicon(io.open("../wuggydict/wuggydict.tsv", mode="r", encoding="utf-8"))
    g.load_lookup_lexicon(io.open("../wuggydict/wuggydict.tsv", mode="r", encoding="utf-8"))
    #     g.set_output_mode('plain')
    print(g.list_output_modes())
    # words=random.sample(gt.lookup_lexicon,10)

    # Here are the set of all legal words
    legal_words = g.lookup_lexicon
    a = words - set(legal_words.keys())

    print("There are {:d} legal words.".format(len(words.intersection(legal_words))))


    def generate_candidates_1worker(words):
        print("{} | Generating {:d} words...".format(current_process().name, len(words)))

        lines = []
        start_time = time()
        for idx, word in enumerate(words):
            j = 0
            if word in legal_words:
                nonword_candidates = set()
                word_start_time = time()
                g.set_reference_sequence(g.lookup(word))
                for i in range(1, 10):
                    g.set_frequency_filter(2 ** i, 2 ** i)
                    g.set_attribute_filter('sequence_length')
                    g.set_attribute_filter('segment_length')
                    # g.set_all_statistics()
                    g.set_statistic('overlap_ratio')
                    # g.set_statistic('plain_length')
                    # g.set_statistic('transition_frequencies')
                    # g.set_statistic('ned1')
                    g.set_statistic('lexicality')
                    g.set_output_mode('syllabic')
                    # it's important not to use the cache because we want wuggy to always 
                    # generate the best matching nonword. The use of the cache would prevent 
                    # to generate multiple times the same nonword and thus lower the quality 
                    # the pairs. (and would cause performance issues on big sets of words)
                    for sequence in g.generate(clear_cache=True):
                        if time() - word_start_time > 50:
                            break

                        try:
                            sequence.encode('utf-8')
                        except UnicodeEncodeError:
                            print('the matching nonword is non-ascii (bad wuggy)')
                            pass
                        else:
                            match = False

                            if high_overlap:
                                n_refseq = len(g.reference_sequence) - 2
                                if (g.statistics['overlap_ratio'] == Fraction(n_refseq - 1, n_refseq) and \
                                        g.statistics['lexicality'] == "N" and sequence not in nonword_candidates \
                                        ):
                                    print('entered high overlap')
                                    match = True
                            else:
                                if (sequence != word
                                        and g.statistics['lexicality'] == "N"
                                        and sequence not in nonword_candidates):
                                    match = True

                            if match:
                                line = [word, sequence]
                                nonword_candidates.add(sequence)
                                # append statistics, doesn't work yet
                                # line.extend(g.statistics.values())
                                # line.extend(g.difference_statistics.values())
                                lines.append('\t'.join(line))
                                j = j + 1
                                if j >= ncandidates:
                                    break
                    if (j >= ncandidates) or (time() - word_start_time > 50):
                        break

            printevry = 100

            if (idx + 1) % printevry == 0 or idx + 1 == len(words):
                print("{} | {:d}/{:d} words processed | {:d} words in {:.4f} seconds".format(
                    current_process().name,
                    idx + 1, len(words),
                    printevry,
                    time() - start_time))
                start_time = time()

        return lines


    # Get word chunks
    nsize = int(len(words) / n_workers)
    if len(words) != nsize * n_workers:
        nsize += 1
    words_chunks = chunks(list(words), nsize)

    print("Start generation with {:d} workers...".format(n_workers))
    #     results = generate_candidates_1worker( words)
    start_time = time()
    with poolcontext(processes=n_workers) as pool:
        try:
            results = pool.map(generate_candidates_1worker, words_chunks)
        except IndexError as err:
            raise (IndexError, err)
    print("Done in {:.4f} seconds !".format(time() - start_time))

    lines = []
    for res in results:
        lines.extend(res)
    print("Generated in total {:d} nonwords.".format(len(lines), ))

    first_line = '\t'.join(['Word', 'Match'])  # + g.statistics + g.difference_statistics)
    output = first_line + '\n' + '\n'.join(lines)

    print("Saving output to " + args.output_path)
    with open(args.output_path, 'w+') as f:
        f.write(output)
