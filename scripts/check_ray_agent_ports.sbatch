#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --hint=nomultithread
#SBATCH --time=00:08:00
#SBATCH --job-name=chia-ray-ports
#SBATCH --output=results/ray-agent-ports-%j.log
#SBATCH --error=results/ray-agent-ports-%j.err

set -euo pipefail
cd "${SLURM_SUBMIT_DIR}"
module load python
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate chia-memscale
export RAY_raylet_start_wait_time_s=120
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
timeout --signal=INT --kill-after=30s 7m python -u scripts/check_ray_agent_ports.py
