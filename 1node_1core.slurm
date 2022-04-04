#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=1node_1core_out.txt
#SBATCH --time=0-1:00:00
#SBATCH --account=COMP90024
#SBATCH --job-name=twitter_1n1c
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

echo ' '
echo 'RUN: 1 node,  1 core'
START_TIME=$SECONDS
module load foss/2020b
module load scipy-bundle/2020.11-python-3.8.6

mpirun python3 main.py --twitter_data_file_path='./bigTwitter.json' --grid_file_path='./data/sydGrid.json' --language_file='./data/language-codes_csv.csv' --batch_size=1000 --out_directory='./results' --slrum-script=$0

ELAPSED_TIME=$(($SECONDS - $START_TIME))
sed "${s/$/$ELAPSED_TIME/}" ./results/1node_1core_timings.txt 