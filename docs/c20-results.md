# C20 RHF/cc-pVDZ calibration results

**Both single-case trials in job `60199152` passed the scientific evidence audit.**
Slurm reported `COMPLETED`, exit `0:0`, elapsed `00:52:21`. The audit returned
`passed: true` with an empty error list. This adds a workload with approximately
24-minute calculations to the execution and validation evidence. It is a fixed
calibration, with no Gemini requests or policy decisions.

## Measurements

| Trial | Harness elapsed (s) | Worker elapsed (s) | Driver wait (s) | Final SCF energy (Hartree) |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1430.007 | 1433.190805 | 1433.281755 | -756.702486554343 |
| 2 | 1426.931 | 1431.744109 | 1432.104141 | -756.702486554343 |

Mean single-case harness time was **1428.469 seconds**, with sample SD
**2.175060 seconds** over two technical repeats. Both measurements are included;
there was no discarded warmup. Each trial ran in a fresh case directory with
P1/E1/B1 and replication 1, using two MPI ranks and two CPUs per rank.

The reference is `-756.702486554389` Hartree and the prespecified energy
acceptance tolerance is `1e-6` Hartree. The reported energy differs from the
reference by approximately `4.6e-11` Hartree in each trial. Acceptance also
requires the expected 20-atom, 280-function RHF workload, exact input bytes,
normal end marker, zero exit code, and consistency with the saved artifacts.
The NWChem input retains its `1e-10` SCF convergence threshold.

Harness time includes launching the case, file handling, and NWChem execution.
Worker time includes the launcher wait and validation; driver wait includes
Ray transport. These intervals overlap and must not be added. Slurm elapsed
also includes setup and cleanup outside the trial intervals. The differences
between these timers do not isolate the cost of the full policy contract or
model decisions; no model was called in this calibration.

## Frozen protocol and reproducible audit

- Implementation budget change: commit `b40464f`.
- Plan committed before submission: `526a407`,
  [`results/c20-calibration-1800s-plan.json`](../results/c20-calibration-1800s-plan.json).
- Plan SHA256: `79b558cadedbf3423d3c97f6a20c845d14725c9f5e2e391a614472e2383b3698`.
- Runtime: Python 3.10.19, Ray 2.54.0, process-local port-forwarding correction.
- Pinned image: `60433343d8c7b7d2d308dc7965f479f456670bfc4c8059a3fdde624e862b6d16`.
- Bounds: 1800-second harness limit per trial, 1860-second Ray wait,
  75-minute regular allocation, 73-minute driver limit plus 30-second kill grace.

The [session](../results/c20-calibration-60199152-1792828523151665996/)
contains the frozen plan, provenance, state, validation, source snapshots,
raw case inputs and outputs, launch records, and Slurm accounting.
The [audit report](../results/c20-calibration-analysis-60199152/summary.json)
contains the measurements above and its validation outcome. The artifact
manifest prepared for publication lists the selected files and their SHA256s;
NWChem integral/restart scratch files are unnecessary for the audit.

Run from the repository root with the matching implementation:

```bash
python scripts/run_workload_calibration.py audit \
  --session results/c20-calibration-60199152-1792828523151665996 \
  --out results/reproduced/c20-calibration-60199152.json
```

The auditor checks the saved source snapshot, plan, image and worker identity,
trial order, raw scientific results, hashes, nested timings, and completion
records. If the runtime protocol changes later, use the recorded revision for
this audit. Hashes check consistency with the preserved records; they do not
authenticate an attacker who rewrites every artifact and its hashes.

## Failed attempts retained

| Job | Observed outcome | Scientific status |
| --- | --- | --- |
| 60082065 | First trial reached the 600-second harness limit; its original output capture did not preserve partial SCF text | No validated trial; cause of that timeout cannot be inferred from its missing output |
| 60147818 | Ray startup failed before any trial | No scientific workload attempted |
| 60148419 | Ray startup failed; diagnostic logs were preserved | No scientific workload attempted |
| 60197597 | Ray connected and NWChem reached SCF iteration 5 at 541.3 s; harness stopped it at 600 s | First trial failed; second unstarted; partial energy is not a final result |
| 60199152 | Both fresh trials completed under the 1800-second protocol | Two successful trials; evidence audit passed |

The Ray-only diagnostic `60193624` independently validated the startup
correction before the successful C20 calibration. Scientific input, reference,
tolerance, MPI layout, and pinned image were preserved when extending the time
budget. Old plans and failed runs are retained as separate evidence; this is
not a claim that the original 600-second protocol succeeded.

## Supported conclusion and limits

The same scientific launcher and raw-output validation workflow executed and
validated two C20 RHF/cc-pVDZ cases in one allocation, extending the demonstration
beyond the short water workload. This is not the proposed 1H9T case. Two repeats
provide a calibration and descriptive spread, not a robust variance estimate,
independent-node replication, scaling result, or configuration landscape.

No Gemini/random comparison, tuning advantage, full contract overhead
comparison, or model/decision-loop overhead was measured on C20. Historical
water API times divided by C20 elapsed time would be an extrapolation, not
observed amortization. Memory use remains unmeasured, and no memory optimization
or adaptation inside a running scientific calculation is demonstrated.

The existing water policy findings remain unchanged: three wins per policy
across six matched pairs in two allocation blocks, with no demonstrated Gemini
advantage. Further C20 policy evaluation needs its own frozen experimental
budget and cost instrumentation. The current calibration does not imply that
all of the policy contract's invariants were exercised on the larger workload.
