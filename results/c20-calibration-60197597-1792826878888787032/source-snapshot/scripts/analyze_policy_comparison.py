#!/usr/bin/env python3
"""Offline, standard-library analysis of compare_loop.py evidence.

No Ray, CHIA, Gemini, Slurm, or workload execution is needed. Historical absolute
measurement paths are deliberately ignored: evidence is read within each session.
"""

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))
from tools.policy_costs import summarize_costs


SOURCE_FILES = (
    "workload/nwchem_benchmark.py", "workload/inputs/water_check.nw",
    "tools/experiment_session.py", "tools/memscale_tool.py", "compare_loop.py",
)
COST_SOURCE_FILES = ("tools/policy_costs.py", "tools/costed_llm.py")
COST_PROVENANCE_FIELDS = ("cost_schema_version", "chia_backend_sha256", "openai_sdk_version",
                          "model_request_timeout_s", "sdk_max_retries", "chia_retries")
CONFIG_FIELDS = ("partitions", "executors", "replication", "batch_size", "placement")
SESSION_FIELDS = (
    "session", "policy", "backend", "job_id", "hostname", "pair_seed", "cohort_id",
    "budget", "attempts", "successes", "failures", "unfinished", "rejected_actions",
    "budget_completed", "validation_passed", "failure_file_present", "analysis_eligible",
    "baseline_cases_per_s", "best_cases_per_s", "best_gain_pct", "best_step",
    "best_executors", "best_batch_size", "first_improvement_step",
    "mean_best_so_far_cases_per_s", "session_elapsed_s", "search_launcher_known_s",
    "search_launcher_elapsed_s", "search_launcher_missing_attempts",
    "cost_accounting_valid", "cost_timing_complete", "session_including_cleanup_s",
    "warmup_elapsed_s", "baseline_elapsed_s", "ray_startup_elapsed_s", "policy_setup_elapsed_s",
    "policy_loop_elapsed_s", "reporting_elapsed_s", "cleanup_elapsed_s", "policy_loop_other_s",
    "search_attempt_elapsed_s", "search_attempt_known_s", "search_attempt_missing_attempts",
    "api_requests", "api_request_elapsed_s", "api_request_known_s", "api_unfinished_requests",
    "api_failed_requests", "api_input_tokens", "api_output_tokens", "api_tokens",
    "api_tokens_known", "api_tokens_missing_requests", "api_cost_usd", "errors", "warnings",
)
PAIR_FIELDS = (
    "block", "backend", "hostname", "pair_seed", "cohort_id", "eligible", "reason",
    "gemini_session", "random_session", "gemini_best_cases_per_s",
    "random_best_cases_per_s", "best_difference_cases_per_s",
    "best_relative_difference_pct", "gain_difference_percentage_points",
    "gemini_session_elapsed_s", "random_session_elapsed_s", "session_difference_s",
    "search_launcher_difference_s", "search_attempt_difference_s", "api_request_difference_s",
    "session_including_cleanup_difference_s", "best_throughput_winner",
)
METRICS = ("best_cases_per_s", "best_gain_pct", "session_elapsed_s",
           "search_launcher_elapsed_s", "mean_best_so_far_cases_per_s",
           "session_including_cleanup_s", "search_attempt_elapsed_s", "api_request_elapsed_s",
           "api_input_tokens", "api_output_tokens", "api_tokens")


def number(value, positive=False):
    return (type(value) in (int, float) and math.isfinite(value)
            and (value > 0 if positive else value >= 0))


def read_object(path, errors):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("expected a JSON object")
        return value
    except (OSError, ValueError) as exc:
        errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
        return {}


def config_key(config):
    return tuple(config.get(k) for k in CONFIG_FIELDS)


def valid_config(config):
    return (isinstance(config, dict) and set(config) == set(CONFIG_FIELDS)
            and all(type(config[k]) is int for k in CONFIG_FIELDS[:-1])
            and config["partitions"] == 3 and config["replication"] == 1
            and config["executors"] in (1, 2, 3) and config["batch_size"] in (1, 2, 3)
            and config["placement"] == "single-node")


def audit_measurement(path, config, backend, errors, expected=None):
    """Check the saved measurement, including all three scientific results."""
    before = len(errors)
    data = read_object(path, errors)
    aggregate = data.get("aggregate", {})
    cases = data.get("cases", [])
    try:
        if (data.get("configuration") != config or data.get("backend") != backend
                or data.get("workload") != "nwchem_water_6-31g_scf"
                or aggregate.get("status") != "success"
                or aggregate.get("cases_total") != 3 or aggregate.get("cases_completed") != 3
                or len(cases) != 3
                or {c["case_id"] for c in cases} != {"p0-r0", "p1-r0", "p2-r0"}):
            raise ValueError("workload, configuration, status, or case count mismatch")
        for case in cases:
            energy = case.get("energy_hartree")
            if (case.get("passed") is not True or case.get("returncode") != 0
                    or type(energy) not in (int, float) or not math.isfinite(energy)
                    or abs(energy - (-75.983998)) >= 1e-5):
                raise ValueError("scientific validation failed")
        for field in ("throughput_cases_per_s", "experiment_elapsed_s", "latency_mean_ms"):
            if not number(aggregate.get(field), positive=True):
                raise ValueError(f"invalid {field}")
        # Elapsed time is rounded to 0.001 s; throughput to 0.000001 cases/s.
        elapsed = aggregate["experiment_elapsed_s"]
        throughput = aggregate["throughput_cases_per_s"]
        if abs(elapsed - 3 / throughput) > 0.00051 + 1.6e-6 / throughput ** 2:
            raise ValueError("throughput inconsistent with three cases and elapsed time")
        if expected is not None and aggregate != expected:
            raise ValueError("state aggregate differs from saved measurement")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        errors.append(f"{path.parent.name}/{path.name}: {exc}")
    return aggregate if len(errors) == before else None


def analyze_session(directory):
    errors, warnings = [], []
    row = dict.fromkeys(SESSION_FIELDS)
    row.update(session=str(directory), analysis_eligible=False,
               failure_file_present=(directory / "failure.json").exists())
    provenance = read_object(directory / "provenance.json", errors)
    state = read_object(directory / "state.json", errors)
    validation = read_object(directory / "validation.json", errors)
    row.update(policy=provenance.get("policy"), backend=provenance.get("backend"),
               job_id=provenance.get("job_id"), hostname=provenance.get("hostname"),
               pair_seed=provenance.get("comparison_pair_seed"),
               budget=provenance.get("experiment_budget"),
               validation_passed=validation.get("passed") is True)
    try:
        policy, backend, budget = row["policy"], row["backend"], row["budget"]
        if policy not in ("gemini", "random") or backend not in ("perlmutter", "native"):
            errors.append("unknown policy or backend")
        if type(budget) is not int or budget not in (3, 4, 5):
            errors.append("invalid comparison experiment budget")
        for key, expected in (("policy", policy), ("backend", backend), ("budget", budget)):
            if state.get(key) != expected:
                errors.append(f"state/provenance mismatch: {key}")
        if state.get("prior_history") != [] or state.get("history_inspected") is not True:
            errors.append("comparison must inspect fresh history")
        if type(row["pair_seed"]) is not int:
            errors.append("missing integer comparison_pair_seed")
        if policy == "random" and provenance.get("random_seed") != row["pair_seed"]:
            errors.append("random_seed differs from comparison_pair_seed")
        hashes = provenance.get("sha256", {})
        cost_version = provenance.get("cost_schema_version")
        source_files = SOURCE_FILES
        if cost_version is not None:
            if type(cost_version) is not int or cost_version != 1:
                errors.append("unsupported cost accounting schema")
            source_files += COST_SOURCE_FILES
            for key in COST_PROVENANCE_FIELDS:
                if key not in provenance or provenance[key] is None:
                    errors.append(f"missing cost provenance: {key}")
            if (provenance.get("sdk_max_retries") != 0 or provenance.get("chia_retries") != 1
                    or not number(provenance.get("model_request_timeout_s"), positive=True)):
                errors.append("invalid model request retry/timeout settings")
        for name in source_files:
            snapshot = directory / "source-snapshot" / name
            if not snapshot.is_file():
                errors.append(f"missing source snapshot: {name}")
            elif hashlib.sha256(snapshot.read_bytes()).hexdigest() != hashes.get(name):
                errors.append(f"source snapshot hash mismatch: {name}")
        for key in ("python", "chia_commit", "model", "hostname"):
            if not provenance.get(key):
                errors.append(f"missing provenance: {key}")
        if backend == "perlmutter" and not provenance.get("recorded_image_id"):
            errors.append("missing recorded image identity")
        if backend == "native" and not provenance.get("packages"):
            errors.append("missing native package provenance")
        signature = {k: provenance.get(k) for k in (
            "backend", "hostname", "python", "model", "chia_commit", "image",
            "recorded_image_id", "experiment_budget", "packages", "nwchem_executable",
            "mpi_ranks_per_case", "threads_per_rank", "history_scope")}
        signature["sha256"] = {name: hashes.get(name) for name in source_files}
        if cost_version is not None:
            signature.update({key: provenance.get(key) for key in COST_PROVENANCE_FIELDS})
        row["cohort_id"] = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:16]

        attempts, baseline = state.get("attempts", []), state.get("baseline", {})
        row.update(attempts=len(attempts), successes=sum(r.get("status") == "success" for r in attempts),
                   failures=sum(r.get("status") == "failed" for r in attempts),
                   unfinished=sum(r.get("status") not in ("success", "failed") for r in attempts))
        row["budget_completed"] = row["attempts"] == budget and row["unfinished"] == 0
        if not row["budget_completed"]:
            errors.append("experiment budget not completed")
        records, valid_records, seen, receipts = [baseline, *attempts], [], set(), set()
        last_receipt = None
        for step, record in enumerate(records):
            config = record.get("configuration")
            if not valid_config(config):
                errors.append(f"step {step}: invalid configuration")
                continue
            if config_key(config) in seen:
                errors.append(f"step {step}: repeated configuration")
            seen.add(config_key(config))
            if record.get("step") != step:
                errors.append(f"step {step}: nonsequential step number")
            receipt = record.get("receipt")
            if not isinstance(receipt, str) or not receipt or receipt in receipts:
                errors.append(f"step {step}: missing or reused receipt")
            else:
                receipts.add(receipt)
            if step and (record.get("observed_receipt") != last_receipt
                         or record.get("selection_source") != policy):
                errors.append(f"step {step}: receipt chain or policy mismatch")
            last_receipt = receipt
            label = f"step-{step}" if step else "baseline"
            if record.get("status") == "success":
                aggregate = audit_measurement(directory / label / "measurement.json", config,
                                              backend, errors, record.get("aggregate", {}))
                if aggregate is not None:
                    valid_records.append(record)
            elif step == 0:
                errors.append("baseline did not succeed")
        if (baseline.get("configuration", {}).get("executors") != 1
                or baseline.get("configuration", {}).get("batch_size") != 1
                or baseline.get("selection_source") != "scripted_baseline"):
            errors.append("baseline must be scripted E1/B1")
        audit_measurement(directory / "warmup" / "measurement.json",
                          baseline.get("configuration"), backend, errors)

        if baseline in valid_records:
            base = baseline["aggregate"]["throughput_cases_per_s"]
            best = max(valid_records, key=lambda r: r["aggregate"]["throughput_cases_per_s"])
            throughput = best["aggregate"]["throughput_cases_per_s"]
            row.update(baseline_cases_per_s=base, best_cases_per_s=throughput,
                       best_gain_pct=100 * (throughput / base - 1), best_step=best["step"],
                       best_executors=best["configuration"]["executors"],
                       best_batch_size=best["configuration"]["batch_size"])
            improving = [r["step"] for r in valid_records if r["step"] > 0
                         and r["aggregate"]["throughput_cases_per_s"] > base]
            row["first_improvement_step"] = min(improving) if improving else None
            valid_by_step = {r["step"]: r for r in valid_records}
            incumbent, trajectory = base, []
            for step in range(1, len(attempts) + 1):
                if step in valid_by_step:
                    incumbent = max(incumbent, valid_by_step[step]["aggregate"]["throughput_cases_per_s"])
                trajectory.append(incumbent)
            row["mean_best_so_far_cases_per_s"] = statistics.mean(trajectory) if trajectory else None

        for field, expected in (("policy", policy), ("requested_experiments", budget),
                                ("measured_agent_experiments", row["successes"]),
                                ("final_receipt_reported", True)):
            if validation.get(field) != expected:
                errors.append(f"validation mismatch: {field}")
        expected_pass = row["budget_completed"] and row["failures"] == 0
        if type(validation.get("passed")) is not bool or validation["passed"] != expected_pass:
            errors.append("validation.passed inconsistent with completed attempts")
        elapsed = validation.get("session_elapsed_s")
        row["session_elapsed_s"] = elapsed if number(elapsed, positive=True) else None
        if row["session_elapsed_s"] is None:
            warnings.append("session time unavailable")
        known = [r.get("launcher_elapsed_s") for r in attempts if number(r.get("launcher_elapsed_s"))]
        row["search_launcher_known_s"] = sum(known) if known else None
        row["search_launcher_missing_attempts"] = len(attempts) - len(known)
        row["search_launcher_elapsed_s"] = sum(known) if len(known) == len(attempts) and attempts else None
        if row["search_launcher_missing_attempts"]:
            warnings.append("search cost is incomplete; unknown durations are not zero")
        reported = validation.get("search_launcher_elapsed_s")
        if (row["search_launcher_elapsed_s"] is not None and number(reported)
                and not math.isclose(reported, row["search_launcher_elapsed_s"], abs_tol=1e-6)):
            errors.append("search launcher total differs from attempt records")
        if (row["session_elapsed_s"] is not None and known
                and row["session_elapsed_s"] + .01 < sum(known)):
            errors.append("session time is shorter than sequential search launchers")
        if row["failure_file_present"]:
            warnings.append("session failure.json retained; inspect its cause")
        events_path = directory / "events.jsonl"
        if events_path.exists():
            events = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
            row["rejected_actions"] = sum(e.get("event") == "rejected" for e in events)
        else:
            warnings.append("event log absent; rejected-action count unavailable")
        if cost_version == 1:
            costs = summarize_costs(directory, policy)
            saved = read_object(directory / "costs.json", errors)
            if costs != saved:
                errors.append("costs.json differs from raw journals and attempt records")
            row.update(cost_accounting_valid=costs["accounting_valid"],
                       cost_timing_complete=costs["timing_complete"])
            if not costs["accounting_valid"]:
                errors.extend("cost accounting: " + e for e in costs["errors"])
            else:
                api, search = costs["model_requests"], costs["search_attempts"]
                row.update(session_including_cleanup_s=costs["session_including_cleanup_s"],
                           policy_loop_other_s=costs["policy_loop_other_s"],
                           search_attempt_elapsed_s=search["elapsed_s"],
                           search_attempt_known_s=search["known_elapsed_s"],
                           search_attempt_missing_attempts=search["missing_attempts"],
                           api_requests=api["started"], api_request_elapsed_s=api["elapsed_s"],
                           api_request_known_s=api["known_elapsed_s"], api_unfinished_requests=api["unfinished"],
                           api_failed_requests=api["failed"], api_input_tokens=api["input_tokens"],
                           api_output_tokens=api["output_tokens"], api_tokens=api["total_tokens"],
                           api_tokens_known=api["total_tokens_known"],
                           api_tokens_missing_requests=api["total_tokens_missing_requests"],
                           api_cost_usd=api["api_cost_usd"])
                for name, values in costs["phases"].items():
                    row[name + "_elapsed_s"] = values["elapsed_s"]
                if not costs["timing_complete"]:
                    warnings.append("instrumented timing is incomplete; missing durations remain unknown")
                if not api["core_usage_complete"]:
                    warnings.append("provider token usage is incomplete; known counts are partial")
                if policy == "gemini" and api["started"]:
                    worker = read_object(directory / "model_backend.json", errors)
                    if any(worker.get(k) != provenance.get(k) for k in ("chia_backend_sha256", "openai_sdk_version")):
                        errors.append("model worker backend identity differs from driver provenance")
    except (KeyError, TypeError, ValueError, AttributeError, OSError) as exc:
        errors.append(f"malformed session: {type(exc).__name__}: {exc}")
    row["analysis_eligible"] = not errors
    row["errors"], row["warnings"] = "; ".join(errors), "; ".join(warnings)
    row["_block"] = provenance.get("comparison_group_id") or provenance.get("job_id")
    return row


def pair_sessions(sessions):
    groups = defaultdict(list)
    def hashable(value):
        return json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
    for row in sessions:
        # A seed is a block label, not proof of a shared allocation/environment.
        block = row["_block"]
        key = (str(block) if block is not None else "unassigned:" + row["session"],
               hashable(row["backend"]), hashable(row["hostname"]), hashable(row["pair_seed"]))
        groups[key].append(row)
    pairs = []
    for key, rows in sorted(groups.items(), key=lambda item: str(item[0])):
        pair = dict.fromkeys(PAIR_FIELDS)
        pair.update(zip(("block", "backend", "hostname", "pair_seed"), key))
        pair["eligible"] = False
        policies = {p: [r for r in rows if r["policy"] == p] for p in ("gemini", "random")}
        for p in policies:
            pair[p + "_session"] = " | ".join(r["session"] for r in policies[p])
        reasons = []
        if any(r["_block"] is None for r in rows):
            reasons.append("missing job_id/comparison_group_id; no automatic native pairing")
        if any(len(policies[p]) != 1 for p in policies):
            reasons.append("requires exactly one Gemini and one random session; no automatic rerun selection")
        if not all(r["analysis_eligible"] for r in rows):
            reasons.append("one or more sessions failed evidence checks or did not complete the budget")
        if len({r["cohort_id"] for r in rows}) != 1:
            reasons.append("budget, recorded environment, or source provenance differs")
        if not reasons:
            g, r = policies["gemini"][0], policies["random"][0]
            pair.update(eligible=True, cohort_id=g["cohort_id"],
                        gemini_best_cases_per_s=g["best_cases_per_s"],
                        random_best_cases_per_s=r["best_cases_per_s"],
                        best_difference_cases_per_s=g["best_cases_per_s"] - r["best_cases_per_s"],
                        best_relative_difference_pct=100 * (g["best_cases_per_s"] / r["best_cases_per_s"] - 1),
                        gain_difference_percentage_points=g["best_gain_pct"] - r["best_gain_pct"],
                        gemini_session_elapsed_s=g["session_elapsed_s"],
                        random_session_elapsed_s=r["session_elapsed_s"])
            for target, source in (("session_difference_s", "session_elapsed_s"),
                                   ("search_launcher_difference_s", "search_launcher_elapsed_s"),
                                   ("search_attempt_difference_s", "search_attempt_elapsed_s"),
                                   ("api_request_difference_s", "api_request_elapsed_s"),
                                   ("session_including_cleanup_difference_s", "session_including_cleanup_s")):
                if g[source] is not None and r[source] is not None:
                    pair[target] = g[source] - r[source]
            delta = pair["best_difference_cases_per_s"]
            pair["best_throughput_winner"] = "gemini" if delta > 0 else "random" if delta < 0 else "tie"
        pair["reason"] = "; ".join(reasons)
        pairs.append(pair)
    return pairs


def describe(values):
    values = [v for v in values if type(v) in (int, float) and math.isfinite(v)]
    return {"n": len(values), "mean": statistics.mean(values) if values else None,
            "sample_sd": statistics.stdev(values) if len(values) > 1 else None,
            "median": statistics.median(values) if values else None,
            "min": min(values) if values else None, "max": max(values) if values else None}


def summarize(sessions, pairs):
    cohorts = {}
    for cohort in sorted({r["cohort_id"] for r in sessions if r["analysis_eligible"]}):
        rows = [r for r in sessions if r["analysis_eligible"] and r["cohort_id"] == cohort]
        paired = [p for p in pairs if p["eligible"] and p["cohort_id"] == cohort]
        cohorts[cohort] = {
            "backend": rows[0]["backend"], "hostname": rows[0]["hostname"], "budget": rows[0]["budget"],
            "policies": {p: {metric: describe(r[metric] for r in rows if r["policy"] == p)
                             for metric in METRICS} for p in ("gemini", "random")},
            "eligible_pairs": len(paired),
            "wins": {p: sum(r["best_throughput_winner"] == p for r in paired)
                     for p in ("gemini", "random", "tie")},
            "paired_differences": {metric: describe(p[metric] for p in paired) for metric in (
                "best_difference_cases_per_s", "best_relative_difference_pct",
                "gain_difference_percentage_points", "session_difference_s", "search_launcher_difference_s",
                "search_attempt_difference_s", "api_request_difference_s", "session_including_cleanup_difference_s")},
        }
    return {
        "schema_version": 1,
        "counts": {"sessions": len(sessions), "analysis_eligible_sessions": sum(r["analysis_eligible"] for r in sessions),
                   "sessions_with_failure_file": sum(r["failure_file_present"] for r in sessions),
                   "failed_attempts": sum(r["failures"] or 0 for r in sessions),
                   "ineligible_sessions": sum(not r["analysis_eligible"] for r in sessions),
                   "eligible_pairs": sum(p["eligible"] for p in pairs),
                   "ineligible_pair_groups": sum(not p["eligible"] for p in pairs)},
        "notes": [
            "Descriptive pilot results, not significance tests or evidence of general policy superiority.",
            "Best includes the fresh baseline; mean_best_so_far averages incumbents after each search attempt.",
            "Performance summaries use evidence-checked, full-budget sessions, including terminal failed attempts; incomplete sessions remain listed.",
            "Policy summaries include eligible unpaired sessions; paired differences use only unambiguous comparable pairs.",
            "Positive paired throughput differences favor Gemini; positive time differences mean Gemini took longer.",
            "Gains use each session's own baseline; gain differences are percentage points, not direct policy speedups.",
            "Session time includes warmup, baseline, startup, decisions and tools; the recorded timer excludes final cleanup. It is not pure API latency.",
            "Failed-attempt launcher times may be absent. Known sums are lower bounds; incomplete totals remain null.",
            "Legacy API usage remains unknown. Instrumented usage is provider-reported, with partial counts explicit; Gemini dollar cost remains unpriced.",
            "Instrumented session_including_cleanup_s has a different boundary from legacy session_elapsed_s. Nested request/experiment/phase durations must not be added twice.",
            "Memory optimization and within-application adaptation are not measured.",
            "Pairing checks recorded provenance. recorded_image_id is historical; the mutable tag is not independently resolved here.",
            "Measurement JSON and source snapshots are checked; original console output and installed runtime identity are not re-audited.",
        ],
        "cohorts": cohorts,
    }


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def analyze(results_root, out_dir):
    results_root, out_dir = Path(results_root), Path(out_dir)
    if not results_root.is_dir():
        raise ValueError(f"Results root does not exist: {results_root}")
    directories = sorted(p for p in results_root.rglob("comparison-*") if p.is_dir())
    if not directories:
        raise ValueError("No comparison-* session directories found")
    sessions = [analyze_session(d) for d in directories]
    pairs = pair_sessions(sessions)
    summary = summarize(sessions, pairs)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(out_dir / "sessions.csv", SESSION_FIELDS, sessions)
    write_csv(out_dir / "paired.csv", PAIR_FIELDS, pairs)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return sessions, pairs, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-root", type=Path, default=Path("results"))
    parser.add_argument("--out-dir", type=Path, default=Path("results/policy-analysis"))
    parser.add_argument("--strict", action="store_true", help="exit 2 if any session or pair group is ineligible")
    args = parser.parse_args()
    try:
        _, _, summary = analyze(args.results_root, args.out_dir)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    counts = summary["counts"]
    print(json.dumps(counts, sort_keys=True))
    print(f"Wrote sessions.csv, paired.csv, summary.json to {args.out_dir}")
    if args.strict and (counts["ineligible_sessions"] or counts["ineligible_pair_groups"]):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
