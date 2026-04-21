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

# NOTE: keep the Xet cache outside the uploaded collection. Otherwise the upload
# job can end up recursively uploading its own cache content.
UPLOAD_DIR=/mnt/ceph/users/polymathic/manga-mmu-hdf5-to-hats

export HF_XET_CACHE="/mnt/ceph/users/polymathic/tmp-hf-cache-manga-hf-upload"
export HF_XET_HIGH_PERFORMANCE=1
mkdir -p "$HF_XET_CACHE"

# Opt-in wipe of HF_XET_CACHE before running. Enable with `sbatch --export=ALL,CLEAN=1 upload-manga.sbatch`.
if [ "${CLEAN:-0}" = "1" ]; then
  echo "CLEAN=1: wiping ${HF_XET_CACHE}"
  rm -rf "${HF_XET_CACHE}"
fi

srun uvx --from huggingface-hub hf upload-large-folder \
  --repo-type=dataset \
  --exclude="*.fits" \
  --exclude=".cache/**" \
  --exclude="**/.cache/**" \
  --num-workers="${SLURM_CPUS_PER_TASK}" \
  --no-bars \
  hugging-science/mmu_manga \
  "${UPLOAD_DIR}"
