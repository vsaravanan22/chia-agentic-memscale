# CHIA: Bounded Agentic Scientific Experimentation

A bounded agent–runtime interface for sequential configuration experiments on scientific HPC workloads, built with Gemini, CHIA MCP tools, Ray, and NWChem.

## Overview and current status

CHIA connects an agent to a controlled experiment runner. Gemini reads experiment history, selects an untested configuration, requests execution through a CHIA tool, receives measured performance and scientific-validation feedback, and selects the next experiment. The runtime validates requests and controls execution; the agent has no unrestricted shell tool.

The repository slug and `MemScaleTool` API name are retained for compatibility. That tool exposes bounded experiment history and launch operations; it does not implement a separate memory-management layer. Executors and batch size control case concurrency and waves in the NWChem harness. Memory use, memory policies, and adaptation within a running NWChem calculation are not evaluated, so MemScale has been removed from the displayed project title.

**The exhaustive sweep and contract-overhead study are complete and pass their evidence audits.** Job `60056838` measured all nine configurations five times; E3/B3 had the highest mean throughput, 1.547847 cases/s. Job `60080557` completed ten bare/contract pairs; directly timed contract components averaged 145.698 ms, with a mean share of 3.248% of contract-worker time. The mean paired increase in total call time was 5.356%, with a sample SD of 13.536 percentage points. These measurements do not support an "under 1%" claim. See the [results and limitations](docs/oct28-results.md) and [frozen evaluation protocol](docs/oct28-evaluation.md).

Reanalysis of the two published comparison jobs finds **three Gemini wins and three random wins**, reported by job block. Relative to the new sweep's empirical best configuration, Gemini visited and selected E3/B3 in 0/6 historical sessions and random in 2/6; the two random hits repeat seed 103 across the two jobs. These are retrospective configuration comparisons, not six independent allocation replicates. [Confirmation means, spread, and raw points](results/oct28-existing-evidence/confirmation.png) are also available.

A [bounded C20 RHF/cc-pVDZ calibration](docs/larger-workload-calibration.md) is implemented for the optional larger-workload evaluation. It reuses the NWChem launcher with a pinned upstream input and SCF reference. Perlmutter validation and larger-workload policy comparisons are pending.

**The autonomous loop has been validated on NERSC Perlmutter with NWChem 7.2.0.** Job `58656462` completed on September 20, 2026, with exit code `0:0`. Gemini selected four previously untested configurations, and all four experiments passed scientific validation. This is a small water-SCF orchestration demonstration; memory optimization and larger-workload scaling remain research goals.

**GCP validation is also complete:** four independent sessions on September 23, 2026 executed 16 Gemini-selected experiments, all scientifically validated. The best selections improved throughput by 46.2–187.6% over their respective fresh GCP baselines. See [all GCP results and limitations](docs/gcp-results.md), [CSV/JSON evidence](results/gcp-summary/), and [clean VM setup instructions](docs/gcp.md). These are small-workload orchestration measurements, not memory-optimization or cross-platform speedup claims.

## Policy comparison and repeated measurements

**This pilot does not establish a Gemini advantage over random search or the fixed E3/B3 control.**

Job `58835485` contains three matched Gemini/random pairs, with four search attempts per session. Descriptive means across three sessions per policy were:

| Policy | Best observed throughput (cases/s) | Recorded session time (s) |
| --- | ---: | ---: |
| Gemini | 0.676446 | 122.418 |
| Random | 0.721683 | 78.767 |

Session time includes startup, warmup, baseline, decisions, execution, and tool traffic. It is not an isolated model-latency or monetary-cost measurement. See the [comparison protocol](docs/policy-comparison.md) and [analysis outputs](results/policy-analysis/).

Confirmation job `59798274` completed on `nid006468` with exit code `0:0` in 5 minutes 31 seconds. The configurations and randomized trial order were committed before submission. All 60 measured trials passed scientific validation, with five repetitions of four arms for each original pair.

| Original pair | Baseline E1/B1 | Gemini selection | Random selection | Fixed E3/B3 |
| --- | ---: | ---: | ---: | ---: |
| 101 | 0.484946 | 0.704738 | 0.659941 | 1.321297 |
| 102 | 0.455699 | 0.708005 | 0.638202 | 1.391168 |
| 103 | 0.485023 | 0.768294 | 1.283440 | 1.306936 |

Entries are mean throughput in cases/s over five technical repeats. Each trial contained three SCF calculations; an additional three-calculation warmup was excluded. Standard deviations and individual measurements are in the [confirmation evidence](results/confirmation-59798274-1792042495419462330/).

Gemini's selections exceeded their corresponding baseline means, while fixed E3/B3 had the highest mean in every group. Pair 102 used E2/B2 for both policies, so its timing difference is not evidence of better selection. Pair 103 used E3/B3 for both random and the fixed control.

Confirmation session time was 307.725 seconds, including 297.175 seconds of measured-trial attempts and 7.356 seconds of warmup. No new Gemini decisions were made; this measures confirmation costs.

These repeats do not add independent policy-search sessions. That confirmation covers three original search pairs from one allocation and repeats in one new allocation. The confirmation used a pinned image on a different node; historical/new throughput differences cannot be attributed to policy improvements. Memory optimization and adaptation of a running application remain research goals. See the [confirmation protocol and limitations](docs/policy-confirmation.md).

**Live cost accounting was validated in job `59936561`:** six sessions, three matched pairs, and 24 successful search attempts passed the analysis audit with complete cost timing. Gemini made 18 successful API requests totaling 42,821 provider-reported tokens. Mean best throughput was 0.754008 cases/s for Gemini and 0.944467 for random; mean session time before cleanup was 86.823 seconds and 62.367 seconds, respectively. These descriptive results do not establish a Gemini advantage.

See the [cost-accounting results and limitations](docs/policy-cost-accounting.md#live-validation-job-59936561) and [analysis outputs](results/policy-cost-analysis-59936561/). The earlier failed validation attempt, job `59800416`, is retained and disclosed. API request duration and token usage are recorded separately; monetary cost remains unpriced. A broader comparison still requires a separately frozen study protocol.

## Methodology

The implementation follows a state–action–feedback loop:

1. Gemini inspects structured history, available choices, and the latest execution receipt.
2. Gemini chooses executor count and batch size and supplies an experimental hypothesis.
3. A CHIA MCP tool validates the choice, experiment budget, and receipt before launching the benchmark from a Ray worker.
4. The platform launcher executes NWChem: Slurm/Shifter on Perlmutter, or native OpenMPI on GCP.
5. The tool returns measurements and scientific-validation results for the next decision.

Each experiment performs exactly three independent water 6-31g SCF calculations. Partitions are fixed at 3 and replication at 1. Gemini selects executors and batch size from 1, 2, or 3. Executors bound concurrent launches; batch size controls cases per wave. These are harness scheduling controls, not molecular partitions or NWChem memory-policy controls. Each calculation uses two MPI ranks; Perlmutter requests two CPUs per rank, while the native GCP launcher requests a two-core set per calculation. Numerical-library thread counts are set to one.

The runner requires history inspection, rejects repeated or invalid configurations, enforces a finite experiment budget, and chains unique receipts between decisions. Accepted selections and outcomes are saved. The scientific check compares each final SCF energy with `-75.983998` Hartree within `1e-5` Hartree.

## Validated Perlmutter autonomous trajectory

Step 0 is a fresh scripted baseline after a warm-up. Steps 1–4 are Gemini selections; earlier manual measurements were provided only as historical context.

| Step | Selected by | Configuration | Throughput (cases/s) |
| --- | --- | --- | ---: |
| 0 | Fresh scripted baseline | 3P / 1E / B1 | 0.790497 |
| 1 | Gemini | 3P / 1E / B2 | 0.782891 |
| 2 | Gemini | 3P / 1E / B3 | 0.773051 |
| 3 | Gemini | 3P / 2E / B1 | 0.762315 |
| 4 | Gemini | 3P / 2E / B3 | 1.145505 |

P = independent cases (partitions), E = executors, B = batch size.

The best agent-selected configuration achieved **44.9% higher throughput** than the fresh baseline in this single run: `(1.145505 / 0.790497 - 1) × 100`. The validation record confirms four measured agent experiments and successful reporting of the final receipt.

This demonstrates autonomous sequential experimentation. It does not establish statistical significance, global optimality, or superiority over random or grid search. Timings include launch overhead and run-to-run variation. Memory use is unmeasured (`null`); memory optimization, data-placement policies, and NWChem 1H9T scaling have not been demonstrated.

## Public evidence

The complete published session is in [`results/agent-58656462-1789945660187864036/`](results/agent-58656462-1789945660187864036/).

| Evidence | File |
| --- | --- |
| Agent trajectory | [CSV](results/agent-58656462-1789945660187864036/trajectory.csv) / [Markdown](results/agent-58656462-1789945660187864036/trajectory.md) |
| Decisions and outcomes | [events.jsonl](results/agent-58656462-1789945660187864036/events.jsonl) |
| Actual model/tool exchange | [tool_trace.txt](results/agent-58656462-1789945660187864036/tool_trace.txt) |
| Completion checks | [validation.json](results/agent-58656462-1789945660187864036/validation.json) |
| Best observed configuration | [best_config.json](results/agent-58656462-1789945660187864036/best_config.json) |
| Run provenance | [provenance.json](results/agent-58656462-1789945660187864036/provenance.json) |
| Baseline measurement | [measurement.json](results/agent-58656462-1789945660187864036/baseline/measurement.json) |
| Agent measurements | [Step 1](results/agent-58656462-1789945660187864036/step-1/measurement.json), [Step 2](results/agent-58656462-1789945660187864036/step-2/measurement.json), [Step 3](results/agent-58656462-1789945660187864036/step-3/measurement.json), [Step 4](results/agent-58656462-1789945660187864036/step-4/measurement.json) |
| Slurm completion | [slurm-accounting.txt](results/agent-58656462-1789945660187864036/slurm-accounting.txt) |

Earlier **manual** benchmarks are saved in [experiments.csv](results/experiments.csv) and JSON records for jobs [58611234](results/benchmark-58611234.json), [58611317](results/benchmark-58611317.json), and [58611406](results/benchmark-58611406.json). Their approximately 51.5% three-case throughput difference is a manual configuration comparison, separate from the autonomous trajectory above.

## Reproduction on Perlmutter

Prerequisites: a Perlmutter account with CPU allocation access, Conda through the Python module, Shifter, and a Gemini API key with access to the configured model. The published run used Python 3.10.19, Ray 2.54.0, OpenAI SDK 3.15.0, and `gemini-3.5-flash-lite`.

The following setup is for new checkout directories and a new `chia-memscale` environment. Existing users should reuse their patched installation rather than recreate it. These instructions describe the recorded setup; a fresh end-to-end installation has not yet been independently validated. Dependencies are not fully locked, and model availability may change.

```bash
git clone https://github.com/HPCAI-lab/chia-agentic-memscale.git
cd chia-agentic-memscale
module load python
source "$(conda info --base)/etc/profile.d/conda.sh"
conda create -y -n chia-memscale python=3.10.19
conda activate chia-memscale

git clone https://github.com/ucb-bar/chia.git ../chia-memscale-upstream
git -C ../chia-memscale-upstream checkout "$(cat provenance/chia-commit.txt)"
git -C ../chia-memscale-upstream apply --check "$PWD/provenance/gemini-tool-signature.patch"
git -C ../chia-memscale-upstream apply "$PWD/provenance/gemini-tool-signature.patch"
python -m pip install -e ../chia-memscale-upstream 'ray==2.54.0' 'openai==3.15.0'
python -m pip check
mkdir -p results
TMPDIR="$PWD/results" python -m unittest discover -s tests -p 'test_agent_session.py' -v
```

The CHIA patch preserves complete SDK tool-call objects, including Gemini thought signatures, when sending the next assistant message. The upstream commit and compatibility patch are recorded under `provenance/`.

Prepare the NWChem image and wait for `READY`:

```bash
shifterimg -v pull ghcr.io/nwchemgit/nwchem-720.nersc.mpich4.mpi-pr:latest
```

The historical image ID is in [provenance/nwchem-image-id.txt](provenance/nwchem-image-id.txt). The agent-loop launcher above uses a mutable `latest` tag; compare the resolved image ID with the recorded ID before claiming the identical environment. The confirmation, exhaustive-sweep, and contract-overhead protocols instead pin and verify an image ID in their frozen plans.

Review `scripts/run_agent_loop.sbatch` and change its `--account=m5289` directive if needed for your allocation. From the project directory on a login node:

```bash
read -rsp "Paste Gemini API key: " GEMINI_API_KEY
echo
export GEMINI_API_KEY
sbatch --export=ALL scripts/run_agent_loop.sbatch
```

The batch job activates the environment and executes the workload on a compute node. Do not run the live workload directly on a login node. Each submission creates a new session directory; agent decisions and exact performance may differ. Never commit API keys or credential files.

See [docs/agent_loop.md](docs/agent_loop.md) for resource requirements, timeouts, output files, and completion criteria. The earlier multiplication integration check is in [tests/check_gemini_chia.py](tests/check_gemini_chia.py).

## Repository contents

- `loop.py`: agent orchestration, warm-up, baseline, and session validation.
- `tools/`: bounded CHIA tool and experiment-session state management.
- `workload/`: NWChem input and measurement harness.
- `scripts/`: Perlmutter and GCP launchers, plus the GCP evidence auditor.
- `tests/`: session tests and live integration check.
- `provenance/`: upstream CHIA revision, compatibility patch, and image identity.
- `results/`: selected published measurements and agent evidence.
- `docs/`: detailed run instructions.

## Remaining evaluation

Four GCP agent trajectories, two Perlmutter policy-comparison blocks, repeated confirmation, the full nine-configuration sweep, and the paired contract-overhead study are recorded. The current evidence supports bounded execution and an empirical fixed-configuration reference; it does not establish a Gemini selection advantage. Further evaluation needs new policy seeds across more allocations, reference sweeps collected alongside those policy sessions, and a larger validated scientific workload. The exhaustive sweep characterizes the current space; budget-matched grid or offline-tuning policies and broader configuration spaces remain future comparisons. Additional action controls, including partition count and memory/data-placement policies, require further implementation and validation.

Published evidence now includes throughput and scientific checks, recorded session and API costs, retrospective reference regret, contract component timing, and offline injected failures. Live rejection rates, memory behavior, adaptation within a running application, and generalization to larger workloads remain unmeasured. Only the water 6-31g input has a validated reference in this repository. Manuscript title and invariant-label edits still require the paper source; see the [remaining scope](docs/oct28-results.md#remaining-scope).

The native GCP backend has been validated on a standalone VM. It uses NWChem 7.0.2 rather than the Perlmutter container's 7.2.0; compare configurations within each environment, not absolute throughput across platforms. The exact constrained-install recipe added after the runs has not yet been revalidated on a second clean VM.

