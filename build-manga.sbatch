#!/bin/bash

# NOTES
# - Building MaNGA is memory-intensive ==> use the 'mem' partition
# - 'mem' partition has nodes with 3000GB of memory, 96 CPUs, 7-day runtime limit
# - max-bytes overrides max-rows, but max-rows is required arg for back-compat
# - margin-threshold=-1 disables margin caching. We can always build a margin
# cache separately later

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

srun uv run python ./main.py \
  --transformer=manga \
  --name=mmu_manga \
  --input=/mnt/ceph/users/polymathic/MultimodalUniverse/manga/manga \
  --output=/mnt/ceph/users/polymathic/MultimodalUniverse/manga-mmu-hdf5-to-hats \
  --max-rows=100 \
  --max-bytes=1 \
  --tmp-dir=/mnt/ceph/users/polymathic/manga-staging-tmp \
  --workers=${SLURM_CPUS_PER_TASK} \
  --row-group-size=1 \
  --margin-threshold=-1 \
  --memory-limit=28G
