#!/bin/bash
#SBATCH --job-name=train-g2p            
#SBATCH --partition=gpu       
#SBATCH --cpus-per-task=5
#SBATCH --mem=10G    
#SBATCH --output=train-g2p-3.log      
#SBTACH --gres=gpu:gtx1080:1
#SBATCH --exclude=puck5

source /shared/apps/anaconda3/etc/profile.d/conda.sh
conda activate g2p-gpu-env-4
# echo $LANG
 LANG=en_US.UTF-8  g2p-seq2seq --train ../models/trainsets/train.g2p --model_dir ../models/g2p   --max_epochs 5 --reinit