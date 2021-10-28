#!/bin/bash
output="../statistic"


output="../statistic"
lang="fr"
mkdir -p ${output}/${lang}

output="../statistic"
n_fam="1"
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv

lang="fr"
mkdir -p ${output}/${lang}

output="../statistic"
n_fam="2"
mkdir -p ${output}/${lang}/cr${n_fam}
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv


n_fam="4"
mkdir -p ${output}/${lang}/cr${n_fam}
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv


n_fam="8"
mkdir -p ${output}/${lang}/cr${n_fam}
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv


n_fam="16"
mkdir -p ${output}/${lang}/cr${n_fam}
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv


n_fam="32"
mkdir -p ${output}/${lang}/cr${n_fam}
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv



n_fam="64"
mkdir -p ${output}/${lang}/cr${n_fam}
str_fam=""
for i in $(seq 0 $((${n_fam} - 1)))
do
    str_fam="${str_fam} cr${i}"
done
echo ${str_fam}
mkdir -p ${output}/${lang}/cr${n_fam}
python3 src/main/script/script_metadict.py --input ../databases/metadict.${lang}.${n_fam}.db --output ${output}/${lang}/cr${n_fam} --n_fam ${n_fam} --lang ${lang} --i ${str_fam} > ${output}/${lang}/cr${n_fam}/statistics.csv
###################
lang="en"