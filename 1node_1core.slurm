#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=1node_1core_out.txt
#SBATCH --time=0-1:00:00
#SBATCH --account=COMP90024
#SBATCH --job-name=twitter_1n1c
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1


cd /home/$USER/COMP90024/slurm

echo ' '
echo 'RUN: 1 node,  1 core'

module load foss/2020b
module load scipy-bundle/2020.11-python-3.8.6

mpirun python3 helloworld.py