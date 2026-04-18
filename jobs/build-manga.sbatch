#!/bin/bash

# NOTES
# - Building MaNGA is memory-intensive ==> use the 'mem' partition
# - 'mem' partition nodes have 3000GB of memory, 96 CPUs, 7-day runtime limit
#   otherwise gen partition nodes have a bit under 1000GB, 128 CPUs, 7-day limit
# - margin-threshold=-1 disables margin caching. We can always build a margin
#   cache separately later
# - highest-healpix-order=12 picked because at order 12 the largest pixel in
#   the MMU parent sample holds 6 rows (see notes/manga-bit-identical-clusters.md);
#   this lets max-rows=6 satisfy hats-import's pixel_threshold check natively
#   without a monkey-patch.

#SBATCH --job-name="mmu-hdf5-to-hats: build MaNGA"
#SBATCH --partition=mem
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --cpus-per-task=96
#SBATCH --time=7-00:00:00
#SBATCH --exclusive
#SBATCH --output=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats/slurm-logs/slurm-%j.stdout
#SBATCH --error=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats/slurm-logs/slurm-%j.stderr
#SBATCH --chdir=/mnt/home/thehir/projects/george-mmu-hdf5-to-hats

export MALLOC_TRIM_THRESHOLD_=0

CATALOG_NAME=mmu_manga
OUTPUT_DIR=/mnt/ceph/users/polymathic/manga-mmu-hdf5-to-hats
TMP_DIR=/mnt/ceph/users/polymathic/manga-staging-tmp

# Opt-in wipe of tmp/output before running. Enable with `sbatch --export=ALL,CLEAN=1 build-manga.sbatch`.
if [ "${CLEAN:-0}" = "1" ]; then
  echo "CLEAN=1: wiping ${TMP_DIR} and ${OUTPUT_DIR}/${CATALOG_NAME}"
  rm -rf "${TMP_DIR}" "${OUTPUT_DIR}/${CATALOG_NAME}"
fi

srun uv run python ./main.py \
  --transformer=manga \
  --name=${CATALOG_NAME} \
  --input=/mnt/ceph/users/polymathic/MultimodalUniverse/manga/manga \
  --output=${OUTPUT_DIR} \
  --max-rows=6 \
  --tmp-dir=${TMP_DIR} \
  --workers=${SLURM_CPUS_PER_TASK} \
  --row-group-size=1 \
  --highest-healpix-order=12 \
  --margin-threshold=-1 \
  --memory-limit=28G \
  --no-resume
