#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=2
#SBATCH --hint=nomultithread
#SBATCH --time=00:30:00
#SBATCH --job-name=chia-c20-calibrate

set -euo pipefail
cd "${SLURM_SUBMIT_DIR}"
plan_path=${1:?Pass the committed frozen plan path}
results_path=${2:?Pass the results directory}
module load python
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate chia-memscale
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export RAY_raylet_start_wait_time_s=120
export COMEX_MAX_NB_OUTSTANDING=6 FI_CXI_RX_MATCH_MODE=hybrid
export COMEX_EAGER_THRESHOLD=16384 FI_CXI_RDZV_THRESHOLD=16384
export FI_CXI_OFLOW_BUF_COUNT=6 MPICH_SMP_SINGLE_COPY_MODE=CMA
timeout --signal=INT --kill-after=30s 29m python -u scripts/run_workload_calibration.py run \
    --plan "$plan_path" --results-root "$results_path"
