# Larger-workload calibration: C20 bowl RHF/cc-pVDZ

**Status: the first Perlmutter attempt timed out; no C20 case has yet passed
scientific validation.** The output-preservation fix described below has offline
test coverage and still needs a live run.
This is the first step toward the optional larger-workload evaluation. The
existing water policy loop, published evidence, and scientific reference remain
the water workflow. C20 is an explicit additional workload in the same NWChem
launcher and raw-output validator.

## Scientific case

The candidate is a neutral singlet C20 bowl with a spherical cc-pVDZ basis
(280 functions), derived from NWChem's own QA input. The reference SCF energy
is `-756.702486554389` Hartree; the prespecified tolerance is `1e-6` Hartree.
The exact upstream input/output, version distinction, modifications, license
notice, URLs and hashes are preserved in [provenance/c20-bowl](../provenance/c20-bowl/).

This is a larger SCF molecule/basis workload, not the previously mentioned
1H9T case. Its runtime on the allocated Perlmutter node is unknown. The upstream
output comes from a different version/process count and includes subsequent
coupled-cluster work; its total time must not be used as our expected SCF time.

## First attempt and output-preservation fix

Job `60082065` on `nid004187` attempted the first case under plan
`b900ccf7b0fb4c554a1b4f119d8d476931fd1b3cf25755efc2e1f7936df0402b`.
The recorded worker duration was 600.087 seconds and its error type was
`TimeoutExpired`. The second case remained `not_started`; there were zero
successful trials. These facts were reported from the job's saved state and
validation files. The failure establishes that the harness did not complete
within its limit; it does not establish why, or how much time was spent in SCF.

The original harness used `capture_output=True` and wrote `output.txt` only
after the external process returned. Killing the harness lost the captured
output, so this attempt has no saved SCF text. It cannot distinguish slow
calculation, stalled convergence, or launch/MPI/filesystem problems.

The fix redirects each case's stdout and stderr directly to `output.txt` and
saves `launch.json` before the external call. `launcher.log` records the harness
command, budget, exit or wait failure. Bytes already emitted to the file survive
harness termination; bytes still buffered inside NWChem/MPI cannot be recovered.
The raw file is not rewritten on normal completion. `launch.json` is diagnostic
metadata, not part of the scientific result's artifact-hash set.

Keep job `60082065`, its source snapshot and original plan. A diagnostic rerun
uses a new plan file, `results/c20-calibration-streamed-plan.json`, with updated
source hashes. It retains the same image, input, reference, resources, two-trial
schedule and 600-second limit. Partial logs are diagnostic evidence and are never
accepted as successful scientific measurements. A future change to the workload
or budget requires another explicit protocol.

## Fixed calibration protocol

| Item | Frozen setting |
| --- | --- |
| Measured trials | Two sequential independent cases, in fresh directories |
| Work per trial | One C20 SCF calculation; P1 / E1 / B1 / replication 1 |
| MPI/CPU resources | Two MPI ranks, two CPUs per rank; numerical-library threads = 1 |
| Execution | One Ray actor; no automatic task/actor retries |
| Image | Full Shifter ID from `provenance/nwchem-image-id.txt`, verified inside Shifter |
| Scientific acceptance | Exact input hash, zero exit code, expected 280-function/20-atom RHF output, one final SCF energy within tolerance, normal end marker |
| Per-trial timeout | 600 seconds for the harness; Ray wait allows 60 additional seconds for teardown/return |
| Allocation | One CPU node, eight tasks, two CPUs/task, 30-minute Slurm limit |
| Driver limit | 27 minutes, followed by a 30-second kill grace period |
| Warmup | None discarded; report both first and second measurements |
| Models/policies | No Gemini requests and no policy-search decisions |

The allocation matches the existing Ray/Slurm launcher requirements. Only one
two-rank calculation runs at a time. Harness elapsed time includes launch,
input/output and SCF work. Worker time additionally includes validation; driver
wait time includes Ray transport. These nested durations must not be added.

Any failed, interrupted, or timed-out trial stops the study. The attempted trial
and remaining `not_started` row are retained. The existing launcher terminates
its process group on a harness timeout. A Ray wait failure may leave remote work
temporarily unresolved; no further trial is launched, and the batch job exits
so Slurm can clean up its allocation. Unknown worker duration remains `null`.

Two runs can validate the reference and calibrate a budget. They do not establish
a workload ranking, policy advantage, stable variance, or decision-loop overhead.

## Install checks and freeze before submission

Run these checks from the repository root in the `chia-memscale` environment:

```bash
TMPDIR="$PWD/results" python -m unittest discover -s tests -p 'test_*.py' -q &&
python scripts/run_workload_calibration.py preflight &&
bash -n scripts/run_workload_calibration.sbatch
```

Preflight uses the installed Ray package to verify actor options and
serialization. It starts no Ray worker, scientific workload, or network request.
No Gemini key is needed. Review the `--account=m5289` batch directive for your
allocation before committing. Commit the implementation and upstream reference:

```bash
git add -- README.md docs/larger-workload-calibration.md \
  workload/nwchem_benchmark.py workload/inputs/c20_bowl_scf.nw \
  tools/experiment_session.py scripts/run_workload_calibration.py \
  scripts/run_workload_calibration.sbatch tests/test_workload_calibration.py \
  tests/test_agent_session.py tests/test_native_backend.py tests/test_streamed_output.py \
  provenance/c20-bowl &&
git --no-pager diff --cached --check &&
git commit -m "Preserve workload output across calibration timeouts"
```

Create a new plan once; the command refuses to overwrite an existing plan:

```bash
python scripts/run_workload_calibration.py plan \
  --out results/c20-calibration-streamed-plan.json &&
python scripts/run_workload_calibration.py check \
  --plan results/c20-calibration-streamed-plan.json &&
git add -f -- results/c20-calibration-streamed-plan.json &&
git commit -m "Freeze C20 calibration with streamed output"
```

Then submit:

```bash
python scripts/run_workload_calibration.py submit \
  --plan results/c20-calibration-streamed-plan.json
```

Submission and execution check that all frozen runtime/reference files and the
plan match committed bytes. A changed input, source, timeout or schedule is
rejected. Execute through the batch script; the live runner refuses login nodes.
Log files are `results/c20-calibration-JOBID.log` and `.err`. The log prints the
session directory and each completed trial. A disconnected login shell does not
cancel a submitted batch job.

During execution, inspect `trial-0001/launcher.log` and
`trial-0001/cases/nwchem-p0-r0-*/output.txt` inside that session. The case file
is created before launch and grows as NWChem/MPI emits output. An empty file
alone is not proof of a stalled process because upstream output may be buffered.

## Audit the returned results

After Slurm reports completion, use the session directory printed by the log:

```bash
python scripts/run_workload_calibration.py audit \
  --session results/c20-calibration-JOBID-TIMESTAMP \
  --out results/c20-calibration-JOBID-TIMESTAMP/audit.json
```

Replace the directory placeholder with the actual path. Preserve Slurm
accounting alongside the session. The audit checks the frozen protocol, source
snapshot, pinned image, driver/worker identity, trial order, input and raw-output
hashes, scientific results, timing boundaries and completion counts. It retains
partial rows and exits nonzero for an incomplete or inconsistent study. Means
and sample SD are emitted only when both trials pass. Hashes detect edits
relative to preserved records; they are not signatures against an attacker who
rewrites every artifact and its hashes.

Retain `plan.json`, `provenance.json`, `state.json`, `validation.json`, source
snapshots, each trial's `measurement.json` (if present)/`launcher.log`, and each
case's input, `launch.json` (if present), and `output.txt`. Retain failures even
when no measurement or output was produced. NWChem restart/integral scratch files are not required for the
audit and should not be added to Git. Keep the complete local session until the
evidence selection is reviewed.

## Next decision

Use the two measured durations and validation outcomes to decide whether C20
fits the larger study's allocation/time budget. If it does, freeze a separate
protocol for three-case configurations, independent policy sessions, repeated
measurements and cost accounting. That policy integration is not enabled by this
calibration patch. Measure model and runtime overhead during that new study;
dividing historical water API latency by C20 runtime is only an estimate and
must not be presented as observed overhead.

If either run fails or reaches the limit, retain the failure and inspect the
scientific/launcher output before proposing a new workload or budget. Do not
silently relax the tolerance or discard the first run. Historical frozen water
plans deliberately refuse resubmission after runtime source changes; their
published evidence remains auditable from its saved source snapshots.
