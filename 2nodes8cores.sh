#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=0-00:20:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=10G
#SBATCH --output=2nodes8cores.txt

module load Python/3.7.1-GCC-6.2.0 
srun -n 8 python3 hashtag_identifier.py
