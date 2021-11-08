"""
Script to match each word with a nonword candidate in order to balance ngram scores
using random method. The idea is to, for each word, repeatedly choose a random nonword
such that the score is as close to 0.5 as possible. The algorithm is as follows:
-------------------------------------Algorithm----------------------------------------
    Initialize CHOSEN_PAIR=[] 
    At each step:
        - Randomly chose a word W
        - Take the nonword list NWL corresponding to W
        - Compute an objective function OBJ for the CHOSEN_PAIR indicating how well 
            the the score distributes (eg. OBJ = abs(score1-0.5)+...+abs(scoreN-0.5))
        - Repeatedly choose a random nonword NW from NWL:
            - Compute the new objective NEW_OBJ with CHOSEN_PAIR + (W, NW)
            - If NEW_OBJ < OBJ :
                CHOSEN_PAIR += (W, NW)
                break
---------------------------------------------------------------------------------------
Given a file with several candidates for each word, and another file with several scores possible (unigram/bigram/unigramphone/bigramphone/...) for each word/nonword,
this script chooses for each word one corresponding nonword such that the distribution of the scores is 
equilibre between word and nonword.
EXTENSION - BALANCE THE SCORES BY LENGTH AND/OR FREQUENCY
This script allows to further balance the scores by sub-categories such and length and frequency of the coressponding word. The length and/or frequency of the words
can be given in another file 'categories.txt' (if this file not given, balance like normally).
ATTENTION: For some word with short length, the scores may not be balanced due to the lack of good candidates.

Usage: python balance_scores_random.py word_pairs.txt scores.txt output.txt --categories categories.txt

Example of input files:
    -word_pair.txt (note that the first line should be 'Word Match'):
        Word    Match
        brick   blick
        brick   brikc
        brick   grick
        clean   clenr
        clean   cpean
    -scores.txt (note that the first line should be 'Word score1 score2 ... scoreN'):
        Word score_unigram score_bigram
        brick -27.5 -28.9
        blick -25.46 -23.20
        brikc -28.9 -25.9
        grick -29.4 -28.3
        clean -15.2 -12.4
        clenr -20.615 -12.25
        cpean -12.646 -14.12
    - categories.txt (note that the first line should be 'Word category1 ... category1',
                      and if category is freqency, please give 'freq' (or smt with 'freq' inside), 
                      some examples  : 'Word freq', 'Word length', 'Word char_length phone_length freq'):
        Word length_char length_phone freq
        brick 5 4 946
        clean 5 4 1565
"""

import argparse
import random


def score_with_equal(logprobs, verbose=False):
    assert len(logprobs) > 0 and len(logprobs) % 2 == 0, \
        "Length of the scores must be a strictly positive even interger!"

    n = len(logprobs) // 2
    true_preds = 0.
    for i in range(n):
        if logprobs[2 * i] > logprobs[2 * i + 1]:
            true_preds += 1
        elif logprobs[2 * i] == logprobs[2 * i + 1]:
            true_preds += 0.5

    if verbose:
        print("\t{}/{} ({:.2f}%) of the pairs having score(word) >= score(nonword) !".format(true_preds, n,
                                                                                             100 * true_preds / n))

    return true_preds / n


def scoring_with_categories(word_nonword_list, list_of_score_dict, categoriy_dict=None):
    if categoriy_dict is None:
        categoriy_dict = {word: 1 for word in word_nonword_dict}
    categories = set(categoriy_dict.values())

    n = len(list_of_score_dict)

    all_stats_dict = {}
    for cat in categories:
        all_stats_dict[cat] = []
        all_stats_dict[cat].append(0)  # current_count
        all_stats_dict[cat].append([0.] * n)  # current_true_counts
        all_stats_dict[cat].append([0.] * n)  # current_scores

    for i in range(len(word_nonword_list) // 2):
        word, nonword = word_nonword_list[2 * i:2 * i + 2]
        cat = categoriy_dict[word]

        # Update statistics
        all_stats_dict[cat][0] += 1
        for i in range(n):
            if list_of_score_dict[i][word] > list_of_score_dict[i][nonword]:
                true_score = 1.
            elif list_of_score_dict[i][word] == list_of_score_dict[i][nonword]:
                true_score = 0.5
            else:
                true_score = 0.
            all_stats_dict[cat][1][i] += true_score
            all_stats_dict[cat][2][i] = all_stats_dict[cat][1][i] / all_stats_dict[cat][0]

    for cat in sorted(categories):
        print_scores = " ".join(["{:.2f}".format(100 * acc) for acc in all_stats_dict[cat][2]])
        print("{} ({} pairs) : {}".format(cat, all_stats_dict[cat][0], print_scores))


def rank(freq, interval=[100, 20, 5, 0]):
    """
    Divide the frequency into different ranks
    """
    freq = int(freq)
    assert freq >= 0
    assert sorted(interval, reverse=True) == interval and interval[-1] == 0
    if freq == 0:
        return "FREQRANK-0 (0)"
    else:
        for i in range(len(interval)):
            if interval[i] < freq:
                if i == 0:
                    freq_int = " ({} - inf)".format(interval[i] + 1)
                else:
                    freq_int = " ({} - {})".format(interval[i] + 1, interval[i - 1])
                return "FREQRANK-" + str(len(interval) - i) + freq_int


def sort_word_nonword_list(word_nonword_list):
    word_nonword_dict = {word_nonword_list[2 * i]: word_nonword_list[2 * i + 1] for i in
                         range(len(word_nonword_list) // 2)}
    new_word_nonword_list = []
    for word in sorted(list(word_nonword_dict.keys())):
        new_word_nonword_list.append(word)
        new_word_nonword_list.append(word_nonword_dict[word])
    return new_word_nonword_list


# Define some functions to perform the algo
def f_obj(current_score_list):
    """
    Objective function to be optimized, ideally it should be zero when all the scores equal to 0.5.
    Here we set it to be sum(abs(score1-0.5) + ... + abs(scoreN-0.5))
    """
    obj = 0.
    for score in current_score_list:
        obj += abs(score - 0.5)
    return obj


def choose_nonword_random_with_objective_with_category(list_of_score_dict,
                                                       word_nonword_dict,
                                                       category_dict=None):
    """
    Main function of the algorithm described above. Also balance within
    the categories of the words if given, for ex:
        { the : highfreq,
          boy : highfreq,
          arc : lowfreq,
          ...}
    """
    if category_dict is None:
        category_dict = {word: 1 for word in word_nonword_dict}
    categories = set(category_dict.values())

    n = len(list_of_score_dict)

    succeed_count = 0
    all_count = 0
    all_stats_dict = {}
    for cat in categories:
        all_stats_dict[cat] = []
        all_stats_dict[cat].append(0)  # current_count
        all_stats_dict[cat].append([0.] * n)  # current_true_counts
        all_stats_dict[cat].append([0.] * n)  # current_scores

    word_nonword_chosen = []
    nonword_set = set()
    correct_words = list(word_nonword_dict.keys())  # List of keys
    random.shuffle(correct_words)
    for word in correct_words:
        cat = category_dict[word]
        current_count = all_stats_dict[cat][0]
        current_true_counts = all_stats_dict[cat][1]
        current_scores = all_stats_dict[cat][2]

        nonword_list = word_nonword_dict[word].copy()

        objective = f_obj(current_scores)
        # random.shuffle(nonword_list)
        succeed = 0
        for nonword in nonword_list:
            if nonword not in nonword_set:
                new_scores = []
                for true_count, score_dict in zip(current_true_counts, list_of_score_dict):
                    if score_dict[word] > score_dict[nonword]:
                        true_score = 1.
                    elif score_dict[word] == score_dict[nonword]:
                        true_score = 0.5
                    else:
                        true_score = 0.
                    scr = (true_count + true_score) / (current_count + 1)
                    new_scores.append(scr)

                new_objective = f_obj(new_scores)

                if new_objective < objective:
                    succeed = 1
                    break

        # Randomly redice
        if succeed == 0:
            nonword = random.choice(nonword_list)

        succeed_count += succeed
        all_count += 1

        word_nonword_chosen.append(word)
        word_nonword_chosen.append(nonword)
        nonword_set.add(nonword)

        # Update statistics
        all_stats_dict[cat][0] += 1
        for i in range(n):
            if list_of_score_dict[i][word] > list_of_score_dict[i][nonword]:
                true_score = 1.
            elif list_of_score_dict[i][word] == list_of_score_dict[i][nonword]:
                true_score = 0.5
            else:
                true_score = 0.
            all_stats_dict[cat][1][i] += true_score
            all_stats_dict[cat][2][i] = all_stats_dict[cat][1][i] / all_stats_dict[cat][0]

    print("{}/{} times decrease objective successfully !".format(succeed_count, all_count))
    print("{}/{} pairs finally chosen !".format(all_count, len(word_nonword_dict)))
    print("{} distinct words - {} distinct nonwords".format(all_count, len(nonword_set)))

    return word_nonword_chosen


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="""Given a file with several candidates for each word, and another file 
                                                    with several scores possible (unigram/bigram/unigramphone/bigramphone/...) 
                                                    for each word/nonword, this script chooses for each word one corresponding 
                                                    nonword such that the distribution of the scores is balanced between word and nonword. 
                                                    See the file for an example of input files.""")
    parser.add_argument('word_pair_path', help='the output of the wuggy generation')
    parser.add_argument('scores_path', help='the scores of the word/nonword in the word_pair_path')
    parser.add_argument('output_path', help='output file containing a table of words and matches')
    parser.add_argument('--categories', default=None,
                        help='(optional) the file containing the categories (such as length, frequency, ...) of all the words in the dataset in order to balance according to each sub-category.')
    args = parser.parse_args()

    print("Loading word pairs from {}...".format(args.word_pair_path))
    with open(args.word_pair_path) as f:
        text = f.read()
        lines = text.split("\n")
    assert lines[0].split('\t')[0] == "Word", \
        "The first line of the WORD_PAIR file should be 'Word Match'"
    # Store a dictionary with correct words as keys
    word_nonword_dict = {}
    for line in lines[1:-1]:
        word = line.split('\t')[0]
        nonword = line.split('\t')[1]
        if word not in word_nonword_dict:
            word_nonword_dict[word] = []
        word_nonword_dict[word].append(nonword)
    print("{} words and {} nonwords found!".format(len(word_nonword_dict), len(lines) - 1))

    print("Loading scores from {} ...".format(args.scores_path))
    with open(args.scores_path) as f:
        text = f.read()
        lines = text.split("\n")
    assert lines[0].split('\t')[0] == "Word", \
        "The first line of the SCORES file should be 'Word name_score_1 name_score_2 ... name_score_N'"
    # Create the score dictionaries
    score_names = lines[0].split()[1:]
    n_scores = len(score_names)
    score_dict_list = [{} for _ in range(n_scores)]
    for line in lines[1:-1]:
        word = " ".join(line.split()[:-n_scores])
        scores = line.split()[-n_scores:]
        for i in range(n_scores):
            score_dict_list[i][word] = float(scores[i])

    if args.categories is not None:
        print("Loading length and frequency criteria from {}...".format(args.categories))
        with open(args.categories) as f:
            text = f.read()
            lines = text.split("\n")
        assert lines[0].split('\t')[0] == "Word", \
            "The first line of the CATEGORIES file should be 'Word criterion1 criterion2 ...'"
        # Create the category dictionaries
        category_names = lines[0].split()[1:]
        freq_idx = -1
        for i, cat in enumerate(category_names):
            if "freq" in cat:
                freq_idx = i
        n_catgories = len(category_names)
        category_dicts = {cat: {} for cat in category_names}
        category_combine_dict = {}
        for line in lines[1:-1]:
            word = " ".join(line.split()[:-n_catgories])
            cats = line.split()[-n_catgories:]
            if freq_idx >= 0:
                cats[freq_idx] = rank(cats[freq_idx])
            category_combine_dict[word] = tuple(cats)
            for i in range(n_catgories):
                category_dicts[category_names[i]][word] = cats[i]
    else:
        category_dicts = None
        category_combine_dict = None

    word_nonword_list = choose_nonword_random_with_objective_with_category(score_dict_list, word_nonword_dict,
                                                                           category_dict=category_combine_dict)

    if category_dicts is not None:
        for category_type in category_dicts:
            if "freq" in category_type:
                print("Scores by each category of", category_type,
                      "(the threshold frequencies can be changed by changing the 'interval' inside the function rank().)")
            else:
                print("Scores by each category of", category_type)
            scoring_with_categories(word_nonword_list, score_dict_list, categoriy_dict=category_dicts[category_type])

    # Final score
    for i in range(n_scores):
        print("Final accuracy for score {}:".format(score_names[i]))
        score_list = [score_dict_list[i][word] for word in word_nonword_list]
        score_with_equal(score_list, verbose=True)

    # Write the output
    lines_out = ["Word\tMatch"]
    word_nonword_list = sort_word_nonword_list(word_nonword_list)
    for i in range(len(word_nonword_list) // 2):
        lines_out.append("\t".join([word_nonword_list[2 * i], word_nonword_list[2 * i + 1]]))

    print("Saving output to " + args.output_path)
    with open(args.output_path, 'w+') as f:
        f.write("\n".join(lines_out))
