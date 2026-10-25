#!/usr/bin/env python3
"""Freeze and remeasure pilot selections; no new policy search or model calls."""

import argparse
import csv
import hashlib
import json
import os
import platform
import random
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from scripts.analyze_policy_comparison import analyze_session, describe, pair_sessions, valid_config
from tools.experiment_session import configuration, run_benchmark, write_json

ARMS = ("baseline", "gemini", "random", "fixed_e3b3")
RUNTIME_FILES = ("workload/nwchem_benchmark.py", "workload/inputs/water_check.nw",
                 "tools/experiment_session.py")
IMAGE_ID = re.compile(r"[0-9a-f]{64}")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def plan_digest(plan):
    body = {k: v for k, v in plan.items() if k != "plan_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def schedule(pairs, repeats, seed):
    rng, trials = random.Random(seed), []
    for repeat in range(1, repeats + 1):
        block = [{"pair_seed": pair["pair_seed"], "repeat": repeat, "arm": arm,
                  "configuration": pair["arms"][arm]} for pair in pairs for arm in ARMS]
        rng.shuffle(block)
        for trial in block:
            trials.append({"trial_id": f"trial-{len(trials)+1:04d}", **trial})
    return trials


def build_plan(results_root, source_job, repeats=5, seed=20261015):
    if not str(source_job).isdigit():
        raise ValueError("source-job must be a numeric Slurm job ID")
    if type(repeats) is not int or not 1 <= repeats <= 20 or type(seed) is not int:
        raise ValueError("require 1–20 repetitions and an integer randomization seed")
    directories = sorted(Path(results_root).glob(f"comparison-*-{source_job}-*"))
    if not directories:
        raise ValueError("No comparison sessions found for the requested source job")
    rows = [analyze_session(d) for d in directories]
    pairs = pair_sessions(rows)
    if not pairs or not all(p["eligible"] for p in pairs):
        raise ValueError("Source sessions must pass strict analysis with unambiguous complete pairs")
    if len({r["cohort_id"] for r in rows}) != 1 or any(r["backend"] != "perlmutter" for r in rows):
        raise ValueError("This confirmation runner requires one Perlmutter provenance cohort")
    by_session = {r["session"]: r for r in rows}
    provenance = json.loads((directories[0] / "provenance.json").read_text())
    image_id = provenance["recorded_image_id"]
    if not IMAGE_ID.fullmatch(image_id):
        raise ValueError("Source recorded_image_id is not a full Shifter image ID")
    selected = []
    for pair in pairs:
        arms = {"baseline": configuration(1, 1), "fixed_e3b3": configuration(3, 3)}
        sources = {}
        for policy in ("gemini", "random"):
            row = by_session[pair[policy + "_session"]]
            arms[policy] = configuration(row["best_executors"], row["best_batch_size"])
            sources[policy] = {"session": Path(row["session"]).name, "selected_step": row["best_step"]}
        selected.append({"pair_seed": pair["pair_seed"], "arms": arms, "source_selections": sources})
    selected.sort(key=lambda p: p["pair_seed"])
    evidence = []
    for directory in directories:
        state = json.loads((directory / "state.json").read_text())
        names = ["state.json", "provenance.json", "validation.json", "baseline/measurement.json",
                 "warmup/measurement.json"]
        names += [f"step-{r['step']}/measurement.json" for r in state["attempts"] if r["status"] == "success"]
        names += [str(p.relative_to(directory)) for p in sorted((directory / "source-snapshot").rglob("*")) if p.is_file()]
        evidence.append({"session": directory.name, "sha256": {name: digest(directory / name) for name in names}})
    plan = {
        "schema_version": 1, "created_utc": datetime.now(timezone.utc).isoformat(),
        "source_job": str(source_job), "source_cohort": rows[0]["cohort_id"],
        "backend": "perlmutter", "repeats": repeats, "randomization_seed": seed,
        "image_id": image_id, "historical_image_tag": provenance["image"],
        "runtime_sha256": {name: provenance["sha256"][name] for name in RUNTIME_FILES},
        "source_evidence": evidence, "pairs": selected,
        "warmup_configuration": configuration(1, 1),
        "trials": schedule(selected, repeats, seed),
        "interpretation": "Repeated measurements of frozen historical selections, not additional independent policy sessions. All arms use the same pinned image and harness. Identical arm settings are executed separately.",
    }
    plan["plan_sha256"] = plan_digest(plan)
    validate_plan(plan)
    return plan


def validate_plan(plan):
    if plan.get("schema_version") != 1 or plan.get("backend") != "perlmutter":
        raise ValueError("Unsupported plan")
    if plan.get("plan_sha256") != plan_digest(plan):
        raise ValueError("Plan digest mismatch; do not edit a frozen plan")
    if not IMAGE_ID.fullmatch(plan.get("image_id", "")):
        raise ValueError("Invalid pinned image ID")
    repeats, seed, pairs = plan["repeats"], plan["randomization_seed"], plan["pairs"]
    if type(repeats) is not int or not 1 <= repeats <= 20 or type(seed) is not int or not pairs:
        raise ValueError("Invalid repeat count, seed, or pairs")
    seeds = [p["pair_seed"] for p in pairs]
    if any(type(s) is not int for s in seeds) or len(set(seeds)) != len(seeds):
        raise ValueError("Pair seeds must be unique integers")
    for pair in pairs:
        if set(pair["arms"]) != set(ARMS) or not all(valid_config(c) for c in pair["arms"].values()):
            raise ValueError("Invalid confirmation configurations")
        if pair["arms"]["baseline"] != configuration(1, 1) or pair["arms"]["fixed_e3b3"] != configuration(3, 3):
            raise ValueError("Baseline/control changed")
    if plan["warmup_configuration"] != configuration(1, 1):
        raise ValueError("Warmup changed")
    if plan["trials"] != schedule(pairs, repeats, seed):
        raise ValueError("Trial schedule does not match the declared randomization")


def verify_inputs(plan, results_root, project):
    validate_plan(plan)
    root = Path(results_root).resolve()
    for evidence in plan["source_evidence"]:
        session = evidence["session"]
        if Path(session).name != session or not session.startswith("comparison-"):
            raise ValueError("Invalid source session path")
        for name, expected in evidence["sha256"].items():
            path = (root / session / name).resolve()
            if root not in path.parents or digest(path) != expected:
                raise ValueError(f"Historical evidence changed: {session}/{name}")
    if set(plan["runtime_sha256"]) != set(RUNTIME_FILES):
        raise ValueError("Incomplete runtime provenance")
    for name, expected in plan["runtime_sha256"].items():
        if digest(Path(project) / name) != expected:
            raise ValueError(f"Runtime differs from the selected pilot: {name}")


def require_compute_node():
    if (not os.environ.get("SLURM_JOB_ID") or not socket.gethostname().startswith("nid")
            or int(os.environ.get("SLURM_NTASKS", "0")) < 8
            or int(os.environ.get("SLURM_CPUS_PER_TASK", "0")) < 2):
        raise RuntimeError("Use the confirmation batch script on a Perlmutter CPU compute node (8 tasks, 2 CPUs/task)")


def verify_image(image_id):
    actual = subprocess.check_output(
        ["shifter", "--image", "id:" + image_id, "--module=mpich", "/bin/sh", "-c",
         'printf "%s\\n" "$SHIFTER_IMAGE"'], text=True, timeout=60).strip()
    if actual.removeprefix("id:") != image_id:
        raise RuntimeError("Running Shifter image identity does not match the frozen plan")
    return actual


def execute_trial(state, record, directory, project, plan):
    record.update(status="running", attempt_elapsed_s=None)
    write_json(directory / "state.json", state)
    started = time.monotonic()
    try:
        record.update(run_benchmark(project, directory / record["trial_id"], "id:" + plan["image_id"],
                                    record["configuration"], timeout_s=120, backend="perlmutter"), status="success")
    except Exception as exc:
        record.update(status="failed", error_type=type(exc).__name__)
    except BaseException as exc:
        record.update(status="interrupted", error_type=type(exc).__name__)
        raise
    finally:
        record["attempt_elapsed_s"] = time.monotonic() - started
        write_json(directory / "state.json", state)


def export_results(directory, plan, state):
    by_id = {r["trial_id"]: r for r in state["attempts"]}
    rows, groups = [], {}
    for trial in plan["trials"]:
        record = by_id.get(trial["trial_id"], {})
        metrics = record.get("aggregate", {}) if record.get("status") == "success" else {}
        row = {**trial, "executors": trial["configuration"]["executors"],
               "batch_size": trial["configuration"]["batch_size"],
               "status": record.get("status", "not_started"),
               "throughput_cases_per_s": metrics.get("throughput_cases_per_s"),
               "experiment_elapsed_s": metrics.get("experiment_elapsed_s"),
               "attempt_elapsed_s": record.get("attempt_elapsed_s"),
               "error_type": record.get("error_type"), "measurement_file": record.get("measurement_file")}
        del row["configuration"]
        rows.append(row)
        groups.setdefault((trial["pair_seed"], trial["repeat"]), {})[trial["arm"]] = row
    paired = []
    for (seed, repeat), arms in sorted(groups.items()):
        row = {"pair_seed": seed, "repeat": repeat}
        row.update({arm + "_cases_per_s": arms[arm]["throughput_cases_per_s"] for arm in ARMS})
        g, r = row["gemini_cases_per_s"], row["random_cases_per_s"]
        row["gemini_minus_random_cases_per_s"] = g - r if g is not None and r is not None else None
        paired.append(row)
    for name, data in (("trials.csv", rows), ("paired.csv", paired)):
        with (directory / name).open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(data[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(data)
    summaries = []
    for pair in plan["pairs"]:
        seed = pair["pair_seed"]
        arms = {}
        for arm in ARMS:
            selected = [r for r in rows if r["pair_seed"] == seed and r["arm"] == arm]
            arms[arm] = {
                "configuration": pair["arms"][arm],
                "counts": {status: sum(r["status"] == status for r in selected)
                           for status in ("success", "failed", "running", "interrupted", "not_started")},
                "throughput_cases_per_s": describe(r["throughput_cases_per_s"] for r in selected),
                "attempt_elapsed_s": describe(r["attempt_elapsed_s"] for r in selected),
            }
        summaries.append({"pair_seed": seed, "arms": arms,
                          "paired_repeat_differences_cases_per_s": describe(
                              r["gemini_minus_random_cases_per_s"] for r in paired if r["pair_seed"] == seed)})
    write_json(directory / "summary.json", {
        "plan_sha256": plan["plan_sha256"], "source_job": plan["source_job"], "pairs": summaries,
        "validation": state.get("validation"),
        "notes": ["These are technical repeats of fixed selections; independent policy-session count has not increased.",
                  "One new allocation/node and a pinned image; do not compare raw times directly to historical runs using a mutable tag.",
                  "Throughput summaries describe successful repeats; failure and missing counts are retained.",
                  "Identical configurations under different arms are separate measurements, not independent selection evidence.",
                  "No new Gemini decisions; this phase measures configuration performance and confirmation cost, not policy decision cost."]})


def run_confirmation(plan, results_root, project=PROJECT):
    require_compute_node()
    verify_inputs(plan, results_root, project)
    directory = Path(results_root) / f"confirmation-{os.environ['SLURM_JOB_ID']}-{time.time_ns()}"
    directory.mkdir(parents=True, exist_ok=False)
    print(f"Confirmation directory: {directory}", flush=True)
    write_json(directory / "plan.json", plan)
    state = {"plan_sha256": plan["plan_sha256"], "attempts": [], "warmup": None}
    write_json(directory / "state.json", state)
    provenance = {"hostname": socket.gethostname(), "job_id": os.environ["SLURM_JOB_ID"],
                  "python": platform.python_version(), "image_requested": "id:" + plan["image_id"],
                  "resolved_image_id": None, "source_sha256": {},
                  "environment": {k: os.environ.get(k) for k in (
                      "SLURM_NTASKS", "SLURM_CPUS_PER_TASK", "SLURM_JOB_NODELIST", "OMP_NUM_THREADS",
                      "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COMEX_MAX_NB_OUTSTANDING",
                      "FI_CXI_RX_MATCH_MODE", "COMEX_EAGER_THRESHOLD", "FI_CXI_RDZV_THRESHOLD",
                      "FI_CXI_OFLOW_BUF_COUNT", "MPICH_SMP_SINGLE_COPY_MODE")}}
    for name in (*RUNTIME_FILES, "scripts/run_policy_confirmation.py", "scripts/analyze_policy_comparison.py",
                 "scripts/run_policy_confirmation.sbatch"):
        source = Path(project) / name
        if source.is_file():
            target = directory / "source-snapshot" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
            provenance["source_sha256"][name] = digest(source)
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, capture_output=True, text=True)
    provenance["project_commit"] = result.stdout.strip() if result.returncode == 0 else None
    write_json(directory / "provenance.json", provenance)
    started = time.monotonic()
    try:
        provenance["resolved_image_id"] = verify_image(plan["image_id"])
        write_json(directory / "provenance.json", provenance)
        state["warmup"] = {"trial_id": "warmup", "configuration": plan["warmup_configuration"]}
        execute_trial(state, state["warmup"], directory, project, plan)
        if state["warmup"]["status"] != "success":
            raise RuntimeError("Warmup failed; inspect warmup/launcher.log")
        for trial in plan["trials"]:
            record = dict(trial)
            state["attempts"].append(record)
            execute_trial(state, record, directory, project, plan)
            print(f"{record['trial_id']} pair={record['pair_seed']} repeat={record['repeat']} arm={record['arm']} {record['status']}", flush=True)
    except BaseException as exc:
        state["fatal_error_type"] = type(exc).__name__
        print(f"Confirmation stopped: {type(exc).__name__}; see saved evidence", flush=True)
    finally:
        attempts = state["attempts"]
        passed = (len(attempts) == len(plan["trials"]) and "fatal_error_type" not in state
                  and all(r["status"] == "success" for r in attempts))
        state["validation"] = {
            "passed": passed, "planned_trials": len(plan["trials"]), "attempted_trials": len(attempts),
            "successful_trials": sum(r["status"] == "success" for r in attempts),
            "failed_trials": sum(r["status"] == "failed" for r in attempts),
            "interrupted_trials": sum(r["status"] in ("running", "interrupted") for r in attempts),
            "session_elapsed_s": time.monotonic() - started,
            "trial_attempt_elapsed_s": sum(r["attempt_elapsed_s"] for r in attempts if r.get("attempt_elapsed_s") is not None),
            "warmup_elapsed_s": (state["warmup"] or {}).get("attempt_elapsed_s"),
            "timing_note": "Session starts before image identity check and warmup; ends before final report export. Attempt timing includes failed-launch handling.",
        }
        write_json(directory / "state.json", state)
        write_json(directory / "validation.json", state["validation"])
        export_results(directory, plan, state)
    return directory, passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("plan", help="offline: freeze selected settings and trial order")
    create.add_argument("--source-job", required=True)
    create.add_argument("--results-root", type=Path, default=Path("results"))
    create.add_argument("--out", type=Path, required=True)
    create.add_argument("--repeats", type=int, default=5)
    create.add_argument("--seed", type=int, default=20261015)
    for name in ("check", "submit", "run", "report"):
        command = sub.add_parser(name)
        if name == "report":
            command.add_argument("--session", type=Path, required=True)
        else:
            command.add_argument("--plan", type=Path, required=True)
            command.add_argument("--results-root", type=Path, default=Path("results"))
    args = parser.parse_args()
    try:
        if args.command == "plan":
            plan = build_plan(args.results_root, args.source_job, args.repeats, args.seed)
            verify_inputs(plan, args.results_root, PROJECT)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            with args.out.open("x", encoding="utf-8") as stream:
                json.dump(plan, stream, indent=2, allow_nan=False)
                stream.write("\n")
            print(f"Frozen plan: {args.out}; {len(plan['pairs'])} pairs, {args.repeats} repeats, {len(plan['trials'])} trials + 1 warmup")
            print("Plan SHA256:", plan["plan_sha256"])
            for pair in plan["pairs"]:
                print("Pair", pair["pair_seed"], {arm: f"E{c['executors']}/B{c['batch_size']}" for arm, c in pair["arms"].items()})
        elif args.command == "report":
            plan = json.loads((args.session / "plan.json").read_text())
            validate_plan(plan)
            state = json.loads((args.session / "state.json").read_text())
            export_results(args.session, plan, state)
            print("Rebuilt partial/final reports:", args.session)
        else:
            plan = json.loads(args.plan.read_text())
            verify_inputs(plan, args.results_root, PROJECT)
            if args.command == "check":
                print(f"PASS: frozen plan and source evidence match; {len(plan['trials'])} trials + 1 warmup")
            elif args.command == "submit":
                subprocess.run(["sbatch", "--image=id:" + plan["image_id"],
                                str(PROJECT / "scripts/run_policy_confirmation.sbatch"),
                                str(args.plan.resolve()), str(args.results_root.resolve())], cwd=PROJECT, check=True)
            else:
                directory, passed = run_confirmation(plan, args.results_root)
                print("PASS" if passed else "INCOMPLETE/FAILED", directory)
                if not passed:
                    raise SystemExit(1)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as exc:
        parser.exit(2, f"{type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
