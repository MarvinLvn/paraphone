#!/bin/bash
#SBATCH --job-name=train-p2g            
#SBATCH --partition=gpu       
#SBATCH --cpus-per-task=5    
#SBATCH --mem=10G    
#SBATCH --output=train-p2g-3.log      
#SBTACH --gres=gpu:titanx:1
#SBATCH --exclude=puck5

source /shared/apps/anaconda3/etc/profile.d/conda.sh
conda activate g2p-gpu-env-4
# echo $LANG
 LANG=en_US.UTF-8 g2p-seq2seq --train ../models/trainsets/train.p2g --model_dir ../models/p2g/  --p2g --max_epochs 5 --reinit