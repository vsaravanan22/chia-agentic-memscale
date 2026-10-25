# Larger-workload calibration: C20 bowl RHF/cc-pVDZ

**Status: both C20 calibration trials passed the scientific evidence audit.**
Job `60199152` completed with exit `0:0` in 52 minutes 21 seconds. Harness
times were 1430.007 and 1426.931 seconds, with mean 1428.469 seconds and
sample SD 2.175 seconds. See [results, evidence, and limitations](c20-results.md).
Earlier timeout and startup failures are retained below. The
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
1H9T case. The two calibrated single-case harness times are approximately
23.8 minutes each in the recorded allocation. The upstream
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

## Second attempt and Ray startup diagnostics

Job `60147818` on `nid005999` exited with Slurm status `FAILED`, code `1:0`,
after 2 minutes 55 seconds. Under streamed-output plan
`135ad5b2b6c5deeeb4bf7985c15a661ffe8ee524d5b714b196fc1a145f2c3b7f`,
both trials remained `not_started`, with zero attempts and zero successes.
The recorded session including cleanup was 93.671 seconds. Ray reported that
GCS could not find the node during startup. This is a runtime startup failure;
it provides no evidence about C20 convergence, energy or execution time.
These observations come from the reported accounting, stderr and saved state.

Previously the driver retained only the exception class, while Ray's detailed
logs could remain on the compute node. The updated runner uses a fresh private
directory under `/tmp` for each Ray instance, records its path in provenance,
and copies available Ray system logs into the result session after shutdown.
Short node-local paths avoid Unix socket path-length problems and isolate runs.
At that stage, no startup retry or increased timeout was introduced. The cause of the observed
node-registration failure was then undetermined; the later port-forwarding
diagnostic and integration are described below.

The result session now includes:

- `failure.txt`: the full driver exception traceback when a failure occurs.
- `trial-*/failure.txt`: a worker exception traceback when a workload fails.
- `ray-logs/session_*/`: copied raylet, GCS, dashboard, environment-agent and
  monitor logs when present; live sockets and worker files are not copied.
- `ray-diagnostics.json`: the copied-file list and any copy errors.
- `fatal_error_stage` in `state.json`: image verification, Ray startup,
  worker readiness or the trial identifier at failure.

Diagnostics are separate from scientific result hashes. Their absence is not
evidence of success; copy errors are retained and never suppress the original
failure or cleanup. A hard kill may prevent final log copying. Total session
time now includes this diagnostic export; trial timing boundaries are unchanged.
Offline tests inject startup and copy failures and verify zero worker/workload
launches, preserved errors, shutdown and final validation records.

Retain both failed sessions and their frozen plans. Freeze the updated sources
in `results/c20-calibration-ray-logs-plan.json` before another attempt.

## Fixed calibration protocol

| Item | Frozen setting |
| --- | --- |
| Measured trials | Two sequential independent cases, in fresh directories |
| Work per trial | One C20 SCF calculation; P1 / E1 / B1 / replication 1 |
| MPI/CPU resources | Two MPI ranks, two CPUs per rank; numerical-library threads = 1 |
| Execution | One Ray actor; no automatic task/actor retries |
| Image | Full Shifter ID from `provenance/nwchem-image-id.txt`, verified inside Shifter |
| Scientific acceptance | Exact input hash, zero exit code, expected 280-function/20-atom RHF output, one final SCF energy within tolerance, normal end marker |
| Per-trial timeout | 1800 seconds for the harness; Ray wait allows 60 additional seconds for teardown/return |
| Allocation | One CPU node, eight tasks, two CPUs/task, regular QoS, 75-minute Slurm limit |
| Driver limit | 73 minutes, followed by a 30-second kill grace period, within the 75-minute allocation |
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

Run checks from the repository root in the `chia-memscale` environment:

```bash
TMPDIR="$PWD/results" python -m unittest discover -s tests -p 'test_*.py' -q &&
python scripts/run_workload_calibration.py preflight &&
bash -n scripts/run_workload_calibration.sbatch
```

Preflight checks the installed Ray command builder, actor options and serialization;
it launches no worker, workload or network request. No Gemini key is needed.
The account remains `m5289`. Commit the budget change before creating its plan:

```bash
git add -- README.md docs/larger-workload-calibration.md \
  scripts/run_workload_calibration.py scripts/run_workload_calibration.sbatch \
  tests/test_workload_calibration.py &&
git diff --cached --check &&
git commit -m "Extend C20 calibration budget after observed SCF timeout"
```

Create a new plan once; existing plans are never overwritten:

```bash
python scripts/run_workload_calibration.py plan \
  --out results/c20-calibration-1800s-plan.json &&
python scripts/run_workload_calibration.py check \
  --plan results/c20-calibration-1800s-plan.json &&
git add -f -- results/c20-calibration-1800s-plan.json &&
git commit -m "Freeze 1800-second C20 calibration protocol"
```

Then submit:

```bash
python scripts/run_workload_calibration.py submit \
  --plan results/c20-calibration-1800s-plan.json
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

The two validated calibration times are about 23.8 minutes per case. Assess
that cost before expanding to policy search. A larger policy study requires a separate
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

## Verified startup integration (job 60193624)

The Ray-only diagnostic completed on `nid004171` with a separate worker and
no cleanup errors. It confirmed the missing dashboard HTTP-port forwarding
correction described in [ray-port-forwarding.md](ray-port-forwarding.md).
CLI startup took 118.928 seconds, agent readiness 48.085 seconds, and its
worker probe 87.520 seconds. This is infrastructure evidence, not C20 evidence.

The C20 runner now uses that same owned CLI process, explicit ports and
process-local forwarding helper. Both helper sources are hashed into the plan,
required in HEAD before submission, and copied into each result directory.
The helper accepts the existing C20 allocation of eight tasks with two CPUs
per task, in addition to the diagnostic's four-or-more CPUs per task.

Startup permits 180 seconds for CLI readiness, 90 for agent ports, and 120 for
the calibration actor's ready response. The longer actor readiness limit
accommodates the observed 87.520-second worker probe. The outer driver limit
was 29 minutes for that integration; its allocation was 30 minutes. Both scientific trials
then retained their 600-second harness limit and 660-second Ray wait. The subsequent
SCF timeout and revised budget are documented below. There are no
automatic retries or changes to the scientific input, image, MPI layout,
reference energy, or tolerance.

The driver records the CLI command, ports, forwarding check and startup phase
times in provenance, as well as `ray-start.log` and `port-forwarding.json`.
All startup failures leave trials unstarted. Actor, driver, and owned-launcher
cleanup are each attempted, even if another cleanup operation fails.

That integration was frozen in `results/c20-calibration-forwarded-plan.json`.
It remains a historical record; the current protocol uses the new 1800-second
plan described above. A completed C20 run still requires scientific audit.


## Observed SCF timeout and longer calibration budget

Job `60197597` on `nid004154`, under plan
`dfe0cf6024be55507a12eecbe3931e9ee6750debef44085436498f4f938bd7d3`,
connected to Ray and launched NWChem from worker PID 708969. The user-reported
saved output in
`results/c20-calibration-60197597-1792826878888787032/trial-0001/cases/nwchem-p0-r0-5adh519t/output.txt`
contains five SCF iterations. Iteration 5 reported time 541.3 seconds, energy
-756.7024865534 Hartree and orbital-gradient norm 6.86e-5. The input requested
an SCF convergence threshold of 1e-10. The harness wait reached 600.065 seconds,
then terminated its process group; Slurm reported the step killed. Trial 2
remained unstarted. No completed C20 measurement or scientific PASS was obtained.

A near-reference iteration energy is insufficient for acceptance. NWChem's
[THRESH criterion](https://nwchemgit.github.io/Hartree-Fock-Theory-for-Molecules.html#thresh-convergence-threshold)
uses the orbital-gradient norm; the harness also requires the final results,
normal termination, and zero process exit code. The partial output is retained
as failure evidence, never promoted to a final energy or throughput result.

This observation motivates a new bounded protocol: two fresh single-case runs,
each limited to 1800 seconds, with a 1860-second Ray wait. The batch requests
75 minutes in regular QoS and the driver has a 73-minute limit plus a 30-second
kill grace period. These are limits, not predictions of runtime or convergence.
They provide room for two trials and startup/cleanup; failure still stops the
study, with no automatic retries. The allocation uses the same node/task/CPU
counts. See [NERSC queue limits](https://docs.nersc.gov/jobs/policy/).

The scientific input and its 1e-10 SCF threshold, reference and 1e-6 Hartree
validation tolerance, pinned image, MPI layout, and streamed-output handling
are unchanged. New trials start in fresh directories and do not resume the
interrupted orbitals. Keep the failed session and its source snapshot.
The new protocol was committed as `results/c20-calibration-1800s-plan.json`
in `526a407` before submission. Job `60199152` subsequently completed both
trials and passed its evidence audit with no errors. Full measurements and
limitations are in [c20-results.md](c20-results.md). This establishes calibration
success, not a larger-workload policy advantage.
