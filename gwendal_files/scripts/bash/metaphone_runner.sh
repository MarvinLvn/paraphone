#!/bin/bash
### runner for get syllabified words
############sql ###########
python3 src/main/script/databases.py cmu  > ../fr.dict.parse
python3 src/main/script/databases.py cmu --transcription > ../cmu.transcription
python3 src/main/script/databases.py lexique  > ../lex.csv
python3 src/main/script/databases.py lexique --transcription > ../lexique.transcription 

python3 src/main/script/app.py add \
    --input ../metadict.db \
    --inputs ../fr.dict.parse ../lex.csv ../propres.fr.count.csv \
    --sets cmu lexique propres --overwrite

python3 src/main/script/app.py add \
    --input ../metadict.db \
    --inputs ../intersection.txt \
    --sets "cr0"

python3 src/main/script/app.py add \
    --input ../metadict.db \
    --inputs ../cmu.transcription ../lexique.transcription ../propres.fr.transcription.propre.csv \
    --sets arpa sampa ipa2 \
    --phoneset

time python3 src/main/script/app.py match \
    --input ../metadict.db \
    --functions .src.main.python.utiles.indict_fr \
    --kwargs '{"uun":["cmu", "lexique", "propres"], "ude":["cr0"]}' \
    --wordsets cr0 cmu lexique propres

time python3 src/main/script/app.py replacement \
    --input  ../metadict.db \
    --phoneset sampa  \
    --file ../folding/sampa_folding.txt
    
time python3 src/main/script/app.py folding \
    --input  ../metadict.db \
    --from_phoneset arpa --to_phoneset ipa --file_folding \
    ../folding/phone-folding.txt --phonesets arpa sampa ipa ipa2 --match --overwrite #--drop # --read_match
    
time python3 src/main/script/app.py folding \
    --input  ../metadict.db \
    --from_phoneset sampa --to_phoneset ipa --file_folding \
    ../folding/phone-folding.txt --phonesets arpa sampa ipa ipa2 --match

time python3 src/main/script/app.py replacement \
    --input  ../metadict.db \
    --phoneset ipa2  \
    --file ../folding/ipa2-folding.txt
    
time python3 src/main/script/app.py folding \
    --input  ../metadict.db \
    --from_phoneset ipa2 --to_phoneset ipa --file_folding \
    ../folding/phone-folding.txt --phonesets arpa sampa ipa ipa2 --match


python3 src/main/script/app.py add \
    --input ../metadict.db \
    --inputs ../syll.lex.sampa \
    --sets sampa \
    --syllable \
    --phoneset

python3 src/main/script/app.py add \
    --input ../metadict.db \
    --inputs ../folding/vowels_sampa.txt \
    --sets sampa \
    --vowel \
    --phoneset
    

time python3 src/main/script/app.py replacement \
    --input  ../metadict.db \
    --phoneset sampa  \
    --file ../folding/sampa_folding.txt \
    --syllable

time python3 src/main/script/app.py folding \
    --input  ../metadict.db \
    --from_phoneset sampa --to_phoneset ipa --file_folding \
    ../folding/phone-folding.txt --phonesets arpa sampa ipa ipa2  --syllable --overwrite 
    
    
time python3 src/main/script/app.py folding \
    --input  ../metadict.db \
    --from_phoneset sampa --to_phoneset ipa --file_folding \
    ../folding/phone-folding.txt --phonesets arpa sampa ipa ipa2 --vowel --overwrite 

time python3 src/main/script/app.py replacement \
    --input  ../metadict.db \
    --phoneset ipa \
    --file ../folding/ipa_folding.txt \
    --syllable

time python3 src/main/script/app.py voc \
    --input  ../metadict.db \
    --phoneset sampa 

time python3 src/main/script/app.py voc \
    --input  ../metadict.db \
    --phoneset ipa 

time python3 src/main/script/app.py seq2seq \
    --input  ../metadict.db \
    --phoneset ipa \
    --train \
    --p2g --output ../g2p-models/trainsets/train.p2g
    

time python3 src/main/script/app.py seq2seq \
    --input  ../metadict.db \
    --phoneset ipa \
    --train --output ../g2p-models/trainsets/train.g2p

time python3 src/main/script/app.py replacement \
    --input  ../metadict.db \
    --phoneset ipa  \
    --file ../folding/ipa_folding.txt 
    
time python3 src/main/script/app.py got \
    --input  ../metadict.db \
    --output ../voc/sampa.vowel.txt \
    --table sampa_vowel \
    --select transcription
    
time python3 src/main/script/app.py got \
    --input  ../metadict.db \
    --output ../voc/sampa.onset.txt \
    --table sampa_onset \
    --select onset

time python3 src/main/script/app.py got \
    --input  ../metadict.db \
    --output ../voc/ipa.vowel.txt \
    --table ipa_vowel \
    --select transcription
    
time python3 src/main/script/app.py got \
    --input  ../metadict.db \
    --output ../voc/ipa.onset.txt \
    --table ipa_onset \
    --select onset
    
    
time python3 src/main/script/app.py wordseg \
    --input  ../metadict.db \
    --phoneset ipa \
    --add_onset "k " "f " "m " "k l " "l " "g " "s " "d "\
    --remove_onset "ʁ f " "ʁ k " "ʁ m " "ʁ k l " "ʁ l " "ʁ g " "ʁ s " "ʁ d "