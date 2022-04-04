#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=1node_8core_out.txt
#SBATCH --time=0-1:00:00
#SBATCH --account=COMP90024
#SBATCH --job-name=twitter_1n8c
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8

echo ' '
echo 'RUN: 1 node,  8 core'
START_TIME=$SECONDS
module load foss/2020b
module load scipy-bundle/2020.11-python-3.8.6
slrum_script_path=$(basename $(scontrol show job $SLURM_JOBID | awk -F= '/Command=/{print $2}'))
mpirun python3 main.py --twitter_data_file_path='./bigTwitter.json' --grid_file_path='./data/sydGrid.json' --language_file='./data/language-codes_csv.csv' --batch_size=1000 --out_directory='./results' --slrum-script=$slrum_script_path

ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "elapsed time is $ELAPSED_TIME"
sed -i '$s/$/'"\t $ELAPSED_TIME"'/' ./results/1node_8core_timings.txt
