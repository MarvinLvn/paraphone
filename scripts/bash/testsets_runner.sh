#!/bin/bash


### CREATE databases for each testset
lang="fr"
dict="../fr.dict.parse ../lex.csv ../propres.fr.count.csv"
sets="cmu lexique propres"
sets_comma="\"cmu\", \"lexique\", \"propres\""
fct=".src.main.python.utiles.indict_fr"

n_fam="1"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

n_fam="2"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

n_fam="4"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

n_fam="8"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

n_fam="16"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

n_fam="32"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

n_fam="64"
bash src/main/script/testset_runner.sh "${n_fam}" "${lang}" "${dict}" "${sets}" "${sets_comma}" "${fct}"

###At THE END OF WALL PIPELINE: allow to check which words are removed form non-words
lang="fr"
n_fam="64"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa

n_fam="32"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa

n_fam="16"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa

n_fam="8"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa

n_fam="4"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa

n_fam="2"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa

n_fam="1"
python3 src/main/script/app.py matchpairs --input ../metadict.db --testset ../databases/metadict.${lang}.${n_fam}.db --pairs_file ../wuggydict/testsets/non-words.${n_fam}.txt --output ../wuggydict/testsets/removed.${n_fam}.tsv --phoneset ipa
##########################################################
lang="en"
dict="../cmudict.dict.parse"
sets="cmu"
sets_comma="\"cmu\""
fct=".src.main.python.utiles.indict_en"

