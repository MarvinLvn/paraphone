#!/bin/bash


python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 64 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 32 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 16 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 8 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 4 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 2 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 1 --lang en  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 64 --lang fr  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 32 --lang fr  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 16 --lang fr  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 8 --lang fr  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 4 --lang fr  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 2 --lang fr  --output ../groups

python3 src/main/script/create_families.py ../../../wuggy-pipeline/wuggy_pipeline/data/ --n_fam 1 --lang fr  --output ../groups