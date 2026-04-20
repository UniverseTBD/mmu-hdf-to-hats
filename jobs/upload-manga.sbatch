#!/bin/bash

#SBATCH --job-name="mmu-hdf5-to-hats: upload MaNGA"
#SBATCH --partition=gen
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --cpus-per-task=128
#SBATCH --time=1-00:00:00
#SBATCH --exclusive
#SBATCH --output=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats/slurm-logs/slurm-%j.stdout
#SBATCH --error=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats/slurm-logs/slurm-%j.stderr
#SBATCH --chdir=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats

export HF_XET_CACHE="${TMPDIR:-/tmp}/hf_xet_cache"
mkdir -p "$HF_XET_CACHE"

srun uvx --from huggingface-hub hf upload-large-folder \
  --repo-type=dataset \
  --num-workers="${SLURM_CPUS_PER_TASK}" \
  hugging-science/mmu_manga \
  /mnt/ceph/users/polymathic/manga-mmu-hdf5-to-hats/mmu_manga