"""Durable cost observations; no prompts, credentials, prices, or workload changes."""

import json
import math
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from tools.experiment_session import write_json


SCHEMA_VERSION = 1
PHASES = ("warmup", "baseline", "ray_startup", "policy_setup", "policy_loop",
          "reporting", "cleanup")
TOKEN_FIELDS = ("input_tokens", "output_tokens", "total_tokens",
                "cached_input_tokens", "reasoning_tokens")


def nonnegative(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


class Journal:
    """One writer per journal. A durable start precedes the observed operation."""

    def __init__(self, path):
        self.path = Path(path)

    def append(self, event):
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    @contextmanager
    def span(self, name):
        identifier = uuid.uuid4().hex
        self.append(dict(event="start", span_id=identifier, name=name, time_ns=time.time_ns()))
        started = time.perf_counter()
        detail = {"status": "success"}
        try:
            yield detail
        except BaseException as exc:
            detail.update(status="failed" if isinstance(exc, (Exception, SystemExit)) else "interrupted",
                          error_type=type(exc).__name__)
            status_code = getattr(exc, "status_code", None)
            if type(status_code) is int and 100 <= status_code <= 599:
                detail["http_status"] = status_code
            raise
        finally:
            self.append(dict(**detail, event="end", span_id=identifier, name=name,
                             time_ns=time.time_ns(), elapsed_s=time.perf_counter() - started))


def token_usage(response):
    """Keep only numeric usage fields reported by the provider; absent is unknown."""
    def get(obj, key):
        return obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)

    def count(obj, key):
        value = get(obj, key)
        return value if type(value) is int and value >= 0 else None

    usage = get(response, "usage")
    return {
        "input_tokens": count(usage, "prompt_tokens"),
        "output_tokens": count(usage, "completion_tokens"),
        "total_tokens": count(usage, "total_tokens"),
        "cached_input_tokens": count(get(usage, "prompt_tokens_details"), "cached_tokens"),
        "reasoning_tokens": count(get(usage, "completion_tokens_details"), "reasoning_tokens"),
    }


class TimedCreate:
    """Wrap the SDK call without altering its arguments or response/tool objects."""

    def __init__(self, original, journal_path):
        self.original = original
        self.journal_path = str(journal_path)

    async def __call__(self, *args, **kwargs):
        with Journal(self.journal_path).span("model_request") as record:
            response = await self.original(*args, **kwargs)
            record["usage"] = token_usage(response)
            return response


def read_spans(path, allowed_names, errors):
    starts, ends = {}, {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"{path.name}: {type(exc).__name__}")
        return []
    for line_number, line in enumerate(lines, 1):
        try:
            event = json.loads(line)
            identifier, name, kind = event["span_id"], event["name"], event["event"]
            if (not isinstance(identifier, str) or not identifier or name not in allowed_names
                    or kind not in ("start", "end") or type(event.get("time_ns")) is not int):
                raise ValueError("invalid event identity")
            target = starts if kind == "start" else ends
            if identifier in target:
                raise ValueError("duplicate event")
            if kind == "end":
                if identifier not in starts or starts[identifier]["name"] != name:
                    raise ValueError("end without matching earlier start")
                if event.get("status") not in ("success", "failed", "interrupted"):
                    raise ValueError("invalid outcome")
                if not nonnegative(event.get("elapsed_s")):
                    raise ValueError("invalid duration")
            target[identifier] = event
        except (ValueError, KeyError, TypeError) as exc:
            errors.append(f"{path.name}:{line_number}: {type(exc).__name__}")
    return [{"name": start["name"], "end": ends.get(identifier)}
            for identifier, start in starts.items()]


def duration_summary(entries):
    durations = [e["end"]["elapsed_s"] for e in entries if e["end"] is not None]
    missing = len(entries) - len(durations)
    return {"started": len(entries), "finished": len(durations), "unfinished": missing,
            "known_elapsed_s": sum(durations), "elapsed_s": None if missing else sum(durations),
            "failed": sum(e["end"] is not None and e["end"]["status"] == "failed" for e in entries),
            "interrupted": sum(e["end"] is not None and e["end"]["status"] == "interrupted" for e in entries)}


def summarize_costs(directory, policy):
    directory = Path(directory)
    if policy not in ("gemini", "random"):
        raise ValueError("Unknown policy")
    errors = []
    phases = read_spans(directory / "cost_phases.jsonl", {*PHASES, "session", "random_selection"}, errors)
    requests = read_spans(directory / "model_requests.jsonl", {"model_request"}, errors)
    selected = lambda name: [e for e in phases if e["name"] == name]
    session = duration_summary(selected("session"))
    if session["started"] != 1:
        errors.append("expected exactly one recorded session")
    breakdown = {name: duration_summary(selected(name)) for name in PHASES}
    for name, values in breakdown.items():
        if values["started"] > 1:
            errors.append(f"duplicate top-level phase: {name}")
    if policy == "random" and requests:
        errors.append("random policy unexpectedly recorded model requests")
    if policy == "gemini" and selected("random_selection"):
        errors.append("Gemini policy unexpectedly recorded random selections")
    loop = selected("policy_loop")
    if (policy == "gemini" and loop and loop[0]["end"] is not None
            and loop[0]["end"]["status"] == "success" and not requests):
        errors.append("successful Gemini loop has no recorded model requests")
    for item in selected("session"):
        if item["end"] is not None and item["end"]["status"] == "success":
            for name, values in breakdown.items():
                if values["started"] != 1 or values["unfinished"]:
                    errors.append(f"successful session lacks complete phase: {name}")

    api = duration_summary(requests)
    for field in TOKEN_FIELDS:
        known = []
        for entry in requests:
            end = entry["end"] or {}
            usage = end.get("usage") or {}
            value = usage.get(field) if isinstance(usage, dict) else None
            if end.get("status") == "success" and type(value) is int and value >= 0:
                known.append(value)
        missing = len(requests) - len(known)
        api[field] = sum(known) if not missing else None
        api[field + "_known"] = sum(known)
        api[field + "_missing_requests"] = missing
    api["core_usage_complete"] = all(api[k] is not None for k in TOKEN_FIELDS[:3])
    api["api_cost_usd"] = 0 if policy == "random" and not requests else None

    attempts = None
    state_path = directory / "state.json"
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            attempts = state["attempts"]
            if not isinstance(attempts, list) or not all(isinstance(a, dict) for a in attempts):
                raise ValueError("invalid attempts")
        except (OSError, ValueError, KeyError, TypeError):
            attempts = None
            errors.append("state.json: cannot read attempt costs")
    search = {"attempts": None, "missing_attempts": None, "known_elapsed_s": None, "elapsed_s": None}
    if attempts is not None:
        known = [a["attempt_elapsed_s"] for a in attempts
                 if a.get("status") in ("success", "failed", "interrupted")
                 and nonnegative(a.get("attempt_elapsed_s"))]
        missing = len(attempts) - len(known)
        search.update(attempts=len(attempts), missing_attempts=missing,
                      known_elapsed_s=sum(known), elapsed_s=None if missing else sum(known))

    def residual(total, parts, label):
        if total is None or any(p is None for p in parts):
            return None
        difference = total - sum(parts)
        if difference < -.01:
            errors.append(f"{label}: component times exceed containing interval")
            return None
        return max(0, difference)

    driver_other = residual(session["elapsed_s"], [v["elapsed_s"] for v in breakdown.values()], "driver")
    # On timeout/cancellation a remote request or experiment may finish during cleanup.
    # Its duration cannot be subtracted from the earlier driver wait interval.
    loop_other = None
    if len(loop) == 1 and loop[0]["end"] is not None and loop[0]["end"]["status"] == "success":
        loop_other = residual(breakdown["policy_loop"]["elapsed_s"],
                              [api["elapsed_s"], search["elapsed_s"]], "policy loop")
    valid = not errors
    if not valid:
        # Keep explicitly named partial observations, but never present a corrupt
        # journal's observed subset as a complete time/token/monetary total.
        api["elapsed_s"] = None
        api["core_usage_complete"] = False
        api["api_cost_usd"] = None
        for field in TOKEN_FIELDS:
            api[field] = None
    return {
        "schema_version": SCHEMA_VERSION, "policy": policy, "accounting_valid": valid,
        "timing_complete": (valid and session["unfinished"] == 0
                            and not any(v["unfinished"] for v in breakdown.values())
                            and api["unfinished"] == 0 and search["missing_attempts"] == 0),
        "session_including_cleanup_s": session["elapsed_s"] if valid else None,
        "session": session, "phases": breakdown,
        "random_selection": duration_summary(selected("random_selection")),
        "model_requests": api, "search_attempts": search,
        "driver_other_s": driver_other if valid else None,
        "policy_loop_other_s": loop_other if valid else None,
        "errors": errors,
        "notes": [
            "Top-level phases are inside session. Model requests and search attempts are inside policy_loop; do not add nested intervals twice.",
            "Session starts after imports/preflight/provenance preparation, includes cleanup, and ends before costs.json export; use Slurm accounting for allocation wall time.",
            "validation.json retains its earlier pre-cleanup timing boundary.",
            "Request durations cover SDK/network/provider response handling and usage extraction, not isolated server inference. SDK retries are disabled.",
            "policy_loop_other_s includes Ray/MCP transport, history calls, choice processing and other loop work; it is not pure decision time.",
            "A failed/interrupted policy wait has no residual decomposition: remote work can continue until cleanup cancels it.",
            "Journal writes and checkpoints add observation overhead. Start/end journal writes lie outside the individual measured span.",
            "Missing durations and provider usage are unknown. Known sums are partial observations, not imputed totals.",
            "Token fields are provider-reported counts. Cached/reasoning counts are breakdowns, not additional tokens to add to totals.",
            "Gemini monetary cost is unpriced; token counts are not invoices. Random API cost excludes compute/allocation costs.",
        ],
    }


@contextmanager
def record_session(directory, policy):
    directory = Path(directory)
    for name in ("cost_phases.jsonl", "model_requests.jsonl"):
        (directory / name).touch(exist_ok=False)
    journal = Journal(directory / "cost_phases.jsonl")
    try:
        with journal.span("session"):
            yield journal
    finally:
        write_json(directory / "costs.json", summarize_costs(directory, policy))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Rebuild cost observations from a preserved instrumented session")
    parser.add_argument("--session", type=Path, required=True)
    args = parser.parse_args()
    provenance = json.loads((args.session / "provenance.json").read_text())
    if provenance.get("cost_schema_version") != SCHEMA_VERSION:
        parser.error("This session did not declare the supported cost instrumentation")
    result = summarize_costs(args.session, provenance["policy"])
    write_json(args.session / "costs.json", result)
    print(json.dumps(result, indent=2, allow_nan=False))
    if not result["accounting_valid"]:
        raise SystemExit(2)
