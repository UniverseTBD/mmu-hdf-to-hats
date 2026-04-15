#!/bin/bash

#SBATCH --job-name="mmu-hdf5-to-hats: build MaNGA"
#SBATCH --partition=gen
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --cpus-per-task=128
#SBATCH --time=7-00:00:00
#SBATCH --exclusive
#SBATCH --output=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats/slurm-logs/slurm-%j.stdout
#SBATCH --error=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats/slurm-logs/slurm-%j.stderr
#SBATCH --chdir=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats

srun uv run python ./main.py \
  --transformer=manga \
  --name=mmu_manga_full \
  --input=/mnt/ceph/users/polymathic/MultimodalUniverse/manga/manga \
  --output=/mnt/ceph/users/polymathic/MultimodalUniverse/manga-mmu-hdf5-to-hats \
  --max-rows=10 \
  --tmp-dir=/mnt/ceph/users/polymathic/manga-staging-tmp \
  --workers=${SLURM_CPUS_PER_TASK} \
  --row-group-size=10
