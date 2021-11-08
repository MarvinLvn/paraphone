#!/bin/bash

#### GOAL : GENERATE PAIRS - WORDS NON WORDS
### FILTERS :
## ON WORDS : check that word's trasncription well graphemized with a trained p2g
## ON NON words
# - same number of syllables words-nonwords
# - have good pronunciation P2G2P
# - non words not in corpus


###WUGGY : GENERATE NON-WORDS
## WUGGY needs :
## - a dict in tsv : transcription\ttranscription_syll\tfreqbymillion : ../wuggydict/wuggydict.tsv
## - a list of transcriptions ../wuggydict/words.txt
## a parser for dict : src/main/python/phonetic_fr_ipa.py : see examples in wuggy plugins
## a list of vowels : src/main/python/fr.py , see example in wuggy plugins

cd GenerateWuggy/
# wuggy dict
python3 src/main/python/prewuggy.py wdict --input ../metadict.db --output ../wuggydict/wuggydict.tsv --phoneset ipa

# wuggy lsit of words
python3 src/main/python/prewuggy.py words --input ../metadict.db --from_db ../databases/metadict.fr.1.db --output ../wuggydict/words.txt --phoneset ipa
#python3 src/main/python/prewuggy.py words --input ../metadict.db --from_db ../databases/metadict.fr.64.db --output ../wuggydict/words.txt --phoneset ipa

####WUGGY : generate non-words
## wuggy words : one line per trasncirption
#output : non-words.txt : one line per pairs words-nonwords
#pip install levenshtein # python=2.7
python2.7 src/main/python/generation_candidates_ipa_fr.py ../wuggydict/words.txt  ../wuggydict/non-words.txt --num-workers 18 --num-candidates 10

##FILTERS
## g2p-gpu-env on habilis
#g2p-seq2seq --decode ../wuggydict/test.g2p --model_dir ../models/g2p --output ../wuggydict/results.g2p
##train g2p p2g##train g2p p2g 5 epoch goog rapport
CUDA_VISIBLE_DEVICES=1 g2p-seq2seq --train ../g2p-models/trainsets/train.g2p --model_dir ../g2p-models/g2p   --max_epochs 1 --reinit
CUDA_VISIBLE_DEVICES=0 g2p-seq2seq --train ../g2p-models/trainsets/train.p2g --model_dir ../g2p-models/p2g/  --p2g --max_epochs 1 --reinit
 
 
## filter words (see before)
# format for p2g tsv  : _\ttrasncription : trasncription: space separe phone
python3 src/main/python/g2p2g_tests.py word
# p2g
g2p-seq2seq --decode ../wuggydict/test.word --model_dir ../g2p-models/p2g --output ../wuggydict/results.word --p2g
# filter bad graphemized
python3 src/main/python/g2p2g_tests.py res-word
# filter bad syllabified and format for p2g on nonwords
python3 src/main/python/g2p2g_tests.py test-p2g # > ../wuggydict/test.p2g

# predcit graphem
g2p-seq2seq --decode ../wuggydict/test.p2g --model_dir ../g2p-models/p2g --output ../wuggydict/results.p2g --p2g
#foramt for g2p
python3 src/main/python/g2p2g_tests.py test-g2p > ../wuggydict/test.g2p

#predict phoneme
g2p-seq2seq --decode ../wuggydict/test.g2p --model_dir ../g2p-models/g2p --output ../wuggydict/results.g2p
#filter : non words in words selection; bad rephonemized
python3 src/main/python/filter_g2p.py g2p > ../wuggydict/results.fg2p2g

# format for scoring
python3 src/main/python/g2p2g_tests.py scoring > ../wuggydict/results.antescoring


###SCORING : see Patricia's work
python3 src/main/python/prewuggy.py wfreq --input ../metadict.db --output ../wuggydict/freqdict.tsv --phoneset ipa

cd nonwords-dataset-gen
time python3 scoring.py \
    ../../wuggydict/freqdict.tsv \
    ../../wuggydict/results.antescoring \
    ../../wuggydict/scoring.tsv


time python3 build_cat.py \
    ../../wuggydict/results.antescoring \
    ../../wuggydict/freqdict.tsv \
    ../../wuggydict/categories.antescoring

time python3 balance_scores_with_categories.py \
    ../../wuggydict/results.antescoring \
    ../../wuggydict/scoring.tsv \
    ../../wuggydict/results.postscoring \
    --categories ../../wuggydict/categories.antescoring
    
    
# a last filter : filter on all corpus, non words present as words  matched or not
cd ../
python3 src/main/python/g2p2g_tests.py res-non-word
python3 src/main/python/prewuggy.py all --input ../metadict.db --output ../wuggydict/results.all --phoneset crp --nonwords ../wuggydict/results.gp

## create testset
n_fam="1"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all

n_fam="2"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all

n_fam="4"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all

n_fam="8"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all

n_fam="16"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all

n_fam="32"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all

n_fam="64"
python3 src/main/python/prewuggy.py nonwords --input ../metadict.db --from_db ../databases/metadict.fr.${n_fam}.db --output ../wuggydict/testsets/non-words.${n_fam}.txt --phoneset ipa --nonwords ../wuggydict/results.all


# g ʁ ɑ̃ d d y ʃ ɛ s
# a p ʁ ɛ d ə m ɛ
# y i z
# l ʁ w a b o l j ø
# ʁ ʒ w ɛ̃ d ʁ o z n u z
# akotəmɑ̃    k l a k o t ə m ɑ̃ ̃