#!/usr/bin/env python3
"""Offline audit of the execution-contract invariants reported in the paper's Table 3.

Standard library only; no Ray, CHIA, Gemini, or Slurm. For every comparison session of
the given jobs it checks:

  I1 admissibility  every executed configuration lies in the 3x3 action space
  I2 uniqueness     no configuration repeats within a session (baseline included)
  I3 validity       return codes and SCF energies in measurement.json, and the final
                    energy re-read from every case's raw NWChem output.txt
  I4 provenance     source snapshots and every case's input file against the
                    session's recorded SHA-256 digests
  I5 ordering       receipt chain, sequential steps, and event-log order
                    (history read before the first selection; selected/finished
                    alternate by step; timestamps nondecreasing)
  I6 replay         random-search sessions only: random.Random(seed) over the same
                    untested-candidate ordering reproduces the recorded choices.
                    Not applicable to Gemini, whose decisions are not locally replayable.

I1, I2, the measurement-level part of I3, the snapshot part of I4 and the receipt
chain of I5 reuse analyze_policy_comparison.analyze_session.

Usage, from the repository root:
  python scripts/audit_contract_invariants.py --jobs 58835485
  python scripts/audit_contract_invariants.py --jobs 58835485 59936561 --out results/reproduced/invariants.json
Exit status 0 means every invariant passed for every session.
"""

import argparse
import hashlib
import json
import math
import random
import re
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))
from scripts.analyze_policy_comparison import analyze_session  # noqa: E402

ENERGY = re.compile(r"Total SCF energy\s*=\s*([-+0-9.EeDd]+)")
REFERENCE, TOLERANCE = -75.983998, 1e-5
INPUT = "workload/inputs/water_check.nw"


def configuration(executors, batch_size):
    return dict(partitions=3, executors=executors, replication=1,
                batch_size=batch_size, placement="single-node")


def key(config):
    return tuple(config[k] for k in ("partitions", "executors", "replication", "batch_size"))


def label(config):
    return f"E{config['executors']}/B{config['batch_size']}"


def measurement_dirs(session, state):
    yield session / "warmup"
    yield session / "baseline"
    for record in state["attempts"]:
        yield session / f"step-{record['step']}"


def raw_and_input_checks(session, state, input_sha):
    """I3 raw outputs and I4 per-case inputs. Returns (cases checked, errors)."""
    errors, cases = [], 0
    for directory in measurement_dirs(session, state):
        data = json.loads((directory / "measurement.json").read_text())
        for case in data["cases"]:
            cases += 1
            case_dir = directory / "cases" / Path(case["run_directory"]).name
            text = (case_dir / "output.txt").read_text(errors="replace")
            found = ENERGY.findall(text)
            energy = float(found[-1].replace("D", "E").replace("d", "e")) if found else math.nan
            if (case.get("returncode") != 0 or not math.isfinite(energy)
                    or abs(energy - REFERENCE) >= TOLERANCE
                    or not math.isclose(energy, case["energy_hartree"], rel_tol=0, abs_tol=1e-10)):
                errors.append(f"I3 raw output: {case_dir.relative_to(session)}")
            supplied = case_dir / Path(INPUT).name
            if (not supplied.is_file()
                    or hashlib.sha256(supplied.read_bytes()).hexdigest() != input_sha):
                errors.append(f"I4 case input: {case_dir.relative_to(session)}")
    return cases, errors


def event_order(session, steps):
    events = [json.loads(line) for line in (session / "events.jsonl").read_text().splitlines()
              if line.strip()]
    errors = []
    times = [e["time_ns"] for e in events]
    if times != sorted(times):
        errors.append("I5 event timestamps out of order")
    reads = [i for i, e in enumerate(events) if e["event"] == "history_read"]
    selections = [i for i, e in enumerate(events) if e["event"] == "experiment_selected"]
    if not reads or not selections or reads[0] > selections[0]:
        errors.append("I5 experiment selected before history was read")
    sequence = [(e["event"], e.get("step")) for e in events
                if e["event"] in ("experiment_selected", "experiment_finished")]
    expected = [(kind, step) for step in range(1, steps + 1)
                for kind in ("experiment_selected", "experiment_finished")]
    if sequence != expected:
        errors.append("I5 selected/finished events do not alternate by step")
    rejected = sum(e["event"] == "rejected" for e in events)
    return errors, rejected


def replay(state, seed):
    rng = random.Random(seed)
    used = {key(state["baseline"]["configuration"])}
    choices = []
    for _ in state["attempts"]:
        untested = [configuration(e, b) for e in (1, 2, 3) for b in (1, 2, 3)
                    if key(configuration(e, b)) not in used]
        choice = rng.choice(untested)
        choices.append(choice)
        used.add(key(choice))
    return choices


def audit(session):
    checked = analyze_session(session)
    errors = [f"I1/I2/I3/I4/I5 (analyze_session): {checked['errors']}"] if checked["errors"] else []
    state = json.loads((session / "state.json").read_text())
    provenance = json.loads((session / "provenance.json").read_text())
    cases, problems = raw_and_input_checks(session, state, provenance["sha256"][INPUT])
    errors += problems
    order_errors, rejected = event_order(session, len(state["attempts"]))
    errors += order_errors
    recorded = [r["configuration"] for r in state["attempts"]]
    if provenance["policy"] == "random":
        replayed = replay(state, provenance["random_seed"])
        i6 = "pass" if replayed == recorded else "fail"
        if i6 == "fail":
            errors.append("I6 replay differs: " + " ".join(map(label, replayed)))
    else:
        i6 = "not applicable"
    return {"session": session.name, "job_id": provenance["job_id"], "policy": provenance["policy"],
            "seed": provenance.get("random_seed"), "experiments": 2 + len(state["attempts"]),
            "cases": cases, "rejected_events": rejected, "choices": " ".join(map(label, recorded)),
            "I6_replay": i6, "passed": not errors, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-root", type=Path, default=Path("results"))
    parser.add_argument("--jobs", nargs="+", default=["58835485"])
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    sessions = []
    for job in args.jobs:
        found = sorted(p for p in args.results_root.glob(f"comparison-*-{job}-*") if p.is_dir())
        if not found:
            parser.error(f"no comparison sessions for job {job}")
        sessions += found
    rows = [audit(s) for s in sessions]
    report = {"jobs": args.jobs, "sessions": len(rows),
              "experiments": sum(r["experiments"] for r in rows),
              "cases": sum(r["cases"] for r in rows),
              "rejected_events": sum(r["rejected_events"] for r in rows),
              "all_passed": all(r["passed"] for r in rows), "rows": rows}
    for r in rows:
        status = "PASS" if r["passed"] else "FAIL"
        seed = f" seed {r['seed']}" if r["seed"] is not None else ""
        print(f"{status} {r['job_id']} {r['policy']}{seed}: {r['experiments']} experiments, "
              f"{r['cases']} cases, I6 {r['I6_replay']}, choices {r['choices']}")
        for error in r["errors"]:
            print("   ", error)
    print(f"{'PASS' if report['all_passed'] else 'FAIL'}: {report['sessions']} sessions, "
          f"{report['experiments']} experiments, {report['cases']} cases, "
          f"{report['rejected_events']} rejected events")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + "\n")
    raise SystemExit(0 if report["all_passed"] else 1)


if __name__ == "__main__":
    main()
