#!/bin/bash

n_fam=${1}
lang=${2}
dict=${3}
sets=${4}
sets_comma=${5}
fct=${6}
echo ${n_fam}
echo ${lang}
echo ${dict}
echo ${sets}
echo ${sets_comma}
echo ${fct}
python3 src/main/script/app.py add \
    --input ../databases/metadict.${lang}.${n_fam}.db \
    --inputs ${dict} \
    --sets ${sets} --overwrite
str_fam=""
str_fam_comma=""
for i in $(seq 0 $(($n_fam -1)))
do
    str_fam="${str_fam} cr${i}"
    str_fam_comma="${str_fam_comma}, \"cr${i}\""
    python3 src/main/script/app.py add \
    --input ../databases/metadict.${lang}.${n_fam}.db \
    --inputs ../wordsgroup/${lang}_${n_fam}/${i}.txt \
    --sets "cr${i}"
done
echo ${str_fam_comma}
str_fam_comma=$(echo ${str_fam_comma} | cut -c 2-)
python3 src/main/script/app.py match \
    --input ../databases/metadict.${lang}.${n_fam}.db \
    --functions ${fct} \
    --kwargs "{\"uun\":[${sets_comma}], \"ude\":[${str_fam_comma}]}" \
    --wordsets ${sets} ${str_fam}