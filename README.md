# Paraphone pipeline

This pipeline is aimed a generating synthetic audio recordings of fake words,
base on a corpus of texts

## Setup
It's probably best that you set up paraphone's environment on oberon (some steps
are dependent on the pucks)

```shell
git clone ssh://git@gitlab.cognitive-ml.fr:1022/htiteux/paraphone.git
cd paraphone
# Using a venv. Works with a conda env as well
python3 -m venv venv/
. venv/bin/activate
# this will install all required dependencies and add this package's 
# command-line interface to your env
pip install -e . 
```

## Usage

This is the typical usage on oberon. Make sure you've activated the
package's venv

```shell
# this initializes a workspace
paraphone workspaces/my_workspace/ init --lang fr
# this imports the littaudio and librivox datasets for french, and does not copy them
# (the folders are just symlinked)
paraphone workspaces/myworkspace import dataset /scratch1/projects/InfTrain/dataset/text/FR/Librivox --type librivox --symlink
paraphone workspaces/myworkspace import dataset /scratch1/projects/InfTrain/dataset/text/FR/LittAudio --type littaudio --symlink
# this imports and builds the corpus families from the "matched2.csv" file
paraphone workspaces/myworkspace import families /scratch1/projects/InfTrain/metadata/matched2.csv
# then, we'll need to setup the dictionaries (paraphone automatically knows
# from the workspace's config that it's for french
paraphone workspaces/myworkspace dict-setup
# then, it's tokenization time: text files are cleaned, parsed, and unique words that found a match in the dictionaries
# are collected and stored in a csv file
paraphone workspaces/myworkspace tokenize
# Tokens are then phonemized to IPA, using the dictionaries (and phonemizer as a fallback), 
# as well as the folding rules
paraphone workspaces/myworkspace phonemize
# We can now also syllabify (find the syllabic form) of each word, using the onsets
# from either Lexique or Celex's syllabic forms (depending on Fr or En)
paraphone workspaces/myworkspace syllabify
# the syllabic forms are used by wuggy in its lexicon to generate
# real word/ fake word pairs (10 of them for each real word in this case)
paraphone workspaces/myworkspace wuggy --num_candidates 10
# most of these pairs are trash. We'll need to filter them in some consecutive steps:
# first, init the filtering "subpipeline"
paraphone workspaces/myworkspace filter init
# REST IS TODO
```