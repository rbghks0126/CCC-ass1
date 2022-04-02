#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=2node_8core_out.txt
#SBATCH --time=0-1:00:00
#SBATCH --account=COMP90024
#SBATCH --job-name=twitter_1n8c
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4


cd /home/$USER/COMP90024/slurm

echo ' '
echo 'RUN: 2 node,  4 cores each'

module load foss/2020b
module load scipy-bundle/2020.11-python-3.8.6

mpirun python3 helloworld.py