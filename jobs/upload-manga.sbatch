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

# NOTE: it is recommended to set HF_XET_CACHE to local storage rather than
# running on ceph. The default behaviour is to cache at a .cache/huggingface
# directory inside the directory to be uploaded, and in this case that will be
# on a ceph filesystem. However, setting HF_XET_CACHE to a local directory will
# prevent resuming the upload if the job is resumed on a different node, or the
# cache is cleared, therefore we use the default cache location.
# export HF_XET_CACHE="${TMPDIR:-/tmp}/hf_xet_cache"
# mkdir -p "$HF_XET_CACHE"

UPLOAD_DIR=/mnt/ceph/users/polymathic/manga-mmu-hdf5-to-hats

export HF_XET_CACHE="${UPLOAD_DIR}/.cache/huggingface"
export HF_XET_HIGH_PERFORMANCE=1

# Opt-in wipe of HF_XET_CACHE before running. Enable with `sbatch --export=ALL,CLEAN=1 upload-manga.sbatch`.
if [ "${CLEAN:-0}" = "1" ]; then
  echo "CLEAN=1: wiping ${HF_XET_CACHE}"
  rm -rf "${HF_XET_CACHE}"
fi

srun uvx --from huggingface-hub hf upload-large-folder \
  --repo-type=dataset \
  --exclude="*.fits" \
  --num-workers="${SLURM_CPUS_PER_TASK}" \
  --no-bars \
  hugging-science/mmu_manga \
  "${UPLOAD_DIR}"
