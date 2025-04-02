#!/usr/bin/env bash
#SBATCH --time=24:00:00
#SBATCH --mem=32G
#SBATCH -J SingularitySync
#SBATCH -o /gpfs/scratch/%u/SingularitySync-%j.out

SINGULARITY_REGISTRY_URL="https://raw.githubusercontent.com/brown-bnc/bnc-resource-registry/master/singularity-manifest.yml"
SINGULARITY_SYNC="/gpfs/data/bnc/src/singularity_sync/target/release/singularity_sync"
SIMGS_DIR="/gpfs/data/bnc/simgs/"

export SINGULARITY_CACHEDIR="${HOME}/scratch/singularity"
export SINGULARITY_TMPDIR="${HOME}/scratch/tmp"

main() {
  mkdir -p "${SINGULARITY_TMPDIR}" "${SINGULARITY_CACHEDIR}"
  exec ${SINGULARITY_SYNC} -m "${SINGULARITY_REGISTRY_URL}" --force "${SIMGS_DIR}"
}

main "$@"
