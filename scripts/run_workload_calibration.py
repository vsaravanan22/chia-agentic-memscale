#!/usr/bin/env python3
"""Calibrate two C20 SCF cases on Perlmutter before designing a larger policy study."""
import argparse
import json
import math
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from scripts.run_policy_confirmation import digest, plan_digest, require_compute_node, verify_image, IMAGE_ID
from tools.experiment_session import run_benchmark, validate_benchmark, write_json
from workload.nwchem_benchmark import C20_WORKLOAD, ENERGY, workload_definition, verify_workload_input
from scripts import check_ray_agent_ports as ray_startup

INPUT = "workload/inputs/c20_bowl_scf.nw"
RUNTIME_FILES = (
    "scripts/run_workload_calibration.py", "scripts/run_workload_calibration.sbatch",
    "scripts/check_ray_agent_ports.py", "scripts/ray_port_forwarding.py",
    "tools/experiment_session.py", "workload/nwchem_benchmark.py", INPUT,
    "scripts/run_policy_confirmation.py", "scripts/analyze_policy_comparison.py", "tools/policy_costs.py",
    "provenance/c20-bowl/reference.json", "provenance/c20-bowl/upstream.nw",
    "provenance/c20-bowl/upstream.out", "provenance/c20-bowl/LICENSE.TXT",
)
TRIAL_TIMEOUT = 600
CONFIG = dict(partitions=1, executors=1, replication=1, batch_size=1, placement="single-node")
RAY_DIAGNOSTIC_FILES = (
    "raylet.out", "raylet.err", "gcs_server.out", "gcs_server.err",
    "dashboard.log", "dashboard.out", "dashboard.err", "dashboard_agent.log",
    "dashboard_agent.out", "dashboard_agent.err", "runtime_env_agent.log",
    "runtime_env_agent.out", "runtime_env_agent.err", "monitor.log", "monitor.out", "monitor.err",
)


def save_exception(path, exc):
    """Keep the traceback without locals; failure to write must not skip cleanup."""
    detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    try:
        print(detail, file=sys.stderr, flush=True)
    except OSError:
        pass
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(detail)
    except OSError as error:
        return type(error).__name__
    return None


def preserve_ray_logs(root, directory):
    """Copy this job's system logs before Slurm releases its compute node."""
    report = {"ray_temp_dir": str(root) if root is not None else None, "files": [], "errors": []}
    if root is not None:
        for session in sorted(root.glob("session_*")):
            logs = session / "logs"
            if session.is_symlink() or not session.is_dir() or logs.is_symlink():
                continue
            for name in RAY_DIAGNOSTIC_FILES:
                source = logs / name
                if source.is_symlink() or not source.is_file():
                    continue
                target = directory / "ray-logs" / session.name / name
                try:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
                    report["files"].append(str(target.relative_to(directory)))
                except OSError as exc:
                    report["errors"].append({"file": name, "error_type": type(exc).__name__})
    return report


def protocol():
    definition = workload_definition(C20_WORKLOAD)
    return {"schema_version": 1, "kind": "c20-calibration", "backend": "perlmutter",
            "workload": C20_WORKLOAD, "trial_timeout_s": TRIAL_TIMEOUT,
            "mpi_tasks": 2, "cpus_per_task": 2, "warmup_trials": 0,
            "ray_startup": {"mode": "explicit-ports-with-forwarding", "registration_wait_s": 120,
                            "cli_timeout_s": 180, "agent_timeout_s": 90, "worker_ready_timeout_s": 120},
            "reference_energy_hartree": definition["reference"],
            "tolerance_hartree": definition["tolerance"],
            "trials": [{"trial_id": f"trial-{i:04d}", "configuration": dict(CONFIG)} for i in (1, 2)]}


def verify_reference(project):
    project = Path(project)
    folder = project / "provenance/c20-bowl"
    reference = json.loads((folder / "reference.json").read_text())
    definition = workload_definition(C20_WORKLOAD)
    if (reference["revision"] != "d0d141fdfbf9595b9fd0ec3c7c803c6c5fe020b2"
            or reference["reference_energy_hartree"] != definition["reference"]
            or reference["tolerance_hartree"] != definition["tolerance"]
            or set(reference["files"]) != {"upstream.nw", "upstream.out", "LICENSE.TXT"}):
        raise ValueError("Unexpected upstream scientific reference")
    for name, metadata in reference["files"].items():
        if digest(folder / name) != metadata["sha256"]:
            raise ValueError("Upstream reference bytes changed: " + name)
    energies = ENERGY.findall((folder / "upstream.out").read_text())
    if len(energies) != 1 or float(energies[0]) != definition["reference"]:
        raise ValueError("Upstream output does not support the scientific reference")
    verify_workload_input(project / INPUT, C20_WORKLOAD)
    if reference["derived_input"] != {"path": INPUT, "sha256": digest(project / INPUT)}:
        raise ValueError("Derived input provenance differs")


def build_plan(project, image_id):
    verify_reference(project)
    plan = {**protocol(), "image_id": image_id,
            "runtime_sha256": {name: digest(Path(project) / name) for name in RUNTIME_FILES}}
    plan["plan_sha256"] = plan_digest(plan)
    validate_plan(plan)
    return plan


def validate_plan(plan):
    expected = protocol()
    # JSON comparison distinguishes True from 1 and rejects changed trial order.
    if (plan.get("plan_sha256") != plan_digest(plan)
            or json.dumps({k: plan.get(k) for k in expected}, sort_keys=True) != json.dumps(expected, sort_keys=True)):
        raise ValueError("Plan digest or fixed calibration protocol differs")
    if not isinstance(plan.get("image_id"), str) or not IMAGE_ID.fullmatch(plan["image_id"]):
        raise ValueError("Require a full pinned image ID")
    sources = plan.get("runtime_sha256", {})
    if (set(sources) != set(RUNTIME_FILES)
            or any(not isinstance(v, str) or not IMAGE_ID.fullmatch(v) for v in sources.values())):
        raise ValueError("Incomplete runtime/source hashes")


def verify_inputs(plan, project):
    validate_plan(plan)
    for name, expected in plan["runtime_sha256"].items():
        if digest(Path(project) / name) != expected:
            raise ValueError("Source changed after freezing plan: " + name)
    verify_reference(project)


def require_committed(plan_path, project=PROJECT):
    """The exact code, scientific reference and protocol must precede submission."""
    project = Path(project).resolve()
    relative = Path(plan_path).resolve().relative_to(project)
    for name in (*RUNTIME_FILES, str(relative)):
        result = subprocess.run(["git", "show", "HEAD:" + name], cwd=project,
                                capture_output=True, check=False)
        if result.returncode or result.stdout != (project / name).read_bytes():
            raise ValueError("Commit the exact calibration code and plan before submission: " + name)


class CalibrationWorker:
    def __init__(self, project, directory, plan):
        self.project, self.directory, self.plan = Path(project), Path(directory), plan
        self.next_trial = 0

    def ready(self):
        verify_inputs(self.plan, self.project)
        return {"worker_pid": os.getpid(), "hostname": socket.gethostname(),
                "source_sha256": dict(self.plan["runtime_sha256"])}

    def execute(self, trial_id):
        if (self.next_trial >= len(self.plan["trials"])
                or trial_id != self.plan["trials"][self.next_trial]["trial_id"]):
            raise ValueError("Calibration budget, duplicate, or trial order violation")
        verify_inputs(self.plan, self.project)
        trial = self.plan["trials"][self.next_trial]
        self.next_trial += 1  # A failed attempt still consumes its slot.
        started = time.perf_counter()
        result = {"worker_pid": os.getpid(), "worker_elapsed_s": None}
        try:
            result.update(run_benchmark(self.project, self.directory / trial_id,
                          "id:" + self.plan["image_id"], trial["configuration"],
                          timeout_s=TRIAL_TIMEOUT, backend="perlmutter", workload=C20_WORKLOAD), status="success")
        except Exception as exc:
            result.update(status="failed", error_type=type(exc).__name__)
            diagnostic_error = save_exception(self.directory / trial_id / "failure.txt", exc)
            if diagnostic_error:
                result["diagnostic_error_type"] = diagnostic_error
        finally:
            result["worker_elapsed_s"] = time.perf_counter() - started
        return result


def run_study(plan, results_root, project=PROJECT):
    require_compute_node()
    verify_inputs(plan, project)
    import ray
    directory = Path(results_root).resolve() / f"c20-calibration-{os.environ['SLURM_JOB_ID']}-{time.time_ns()}"
    directory.mkdir(parents=True, exist_ok=False)
    write_json(directory / "plan.json", plan)
    for name, expected in plan["runtime_sha256"].items():
        target = directory / "source-snapshot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((Path(project) / name).read_bytes())
        if digest(target) != expected:
            raise ValueError("Source changed while saving snapshot: " + name)
    provenance = {"job_id": os.environ["SLURM_JOB_ID"], "hostname": socket.gethostname(),
                  "driver_pid": os.getpid(), "python": platform.python_version(), "ray_version": ray.__version__,
                  "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=project, text=True).strip(),
                  "source_sha256": plan["runtime_sha256"], "resolved_image_id": None, "worker": None}
    state = {"plan_sha256": plan["plan_sha256"], "attempts": [
        {**trial, "status": "not_started", "attempt_wait_elapsed_s": None, "worker_elapsed_s": None}
        for trial in plan["trials"]]}
    write_json(directory / "provenance.json", provenance)
    write_json(directory / "state.json", state)
    actor, launcher, ray_temp, started = None, None, None, time.monotonic()
    stage = "image_verification"
    old_handlers = {}

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")

    for signum in (signal.SIGTERM, signal.SIGINT):
        old_handlers[signum] = signal.signal(signum, interrupted)
    print("Session:", directory, flush=True)
    try:
        provenance["resolved_image_id"] = verify_image(plan["image_id"])
        stage = "ray_startup"
        # Short, private, node-local paths also avoid shared temp/socket collisions.
        ray_temp = Path(tempfile.mkdtemp(prefix="chia-ray-", dir="/tmp"))
        provenance["ray_temp_dir"] = str(ray_temp)
        write_json(directory / "provenance.json", provenance)
        state["stage"] = stage
        write_json(directory / "state.json", state)
        print("Ray startup; temporary directory:", ray_temp, flush=True)
        os.environ["RAY_raylet_start_wait_time_s"] = "120"
        _, provenance["port_forwarding_preflight"] = ray_startup.preflight()
        ip = ray_startup.importlib.import_module("ray._private.services").get_node_ip_address()
        ports = ray_startup.select_ports()
        address = ip + ":" + str(ports["--port"])
        command = ray_startup.cli_command(ray_temp, ip, ports, directory / "port-forwarding.json")
        provenance.update(ray_address=address, ray_ports=ports, ray_command=command)
        write_json(directory / "provenance.json", provenance)
        phase = time.monotonic()
        with (directory / "ray-start.log").open("w") as output:
            launcher = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                        start_new_session=True, cwd=project)
        ray_startup.wait_for_cli(launcher, directory / "ray-start.log", timeout=180, ray_tmp=ray_temp)
        provenance["ray_cli_startup_s"] = time.monotonic() - phase
        phase = time.monotonic()
        ray_startup.wait_for_agents(launcher, ip, ports, timeout=90)
        provenance["ray_agent_readiness_s"] = time.monotonic() - phase
        provenance["port_forwarding"] = json.loads((directory / "port-forwarding.json").read_text())
        write_json(directory / "provenance.json", provenance)
        print("Ray CLI and agent ports ready; connecting calibration worker...", flush=True)
        ray.init(address=address,
                 runtime_env={"env_vars": {"PYTHONPATH": str(project) + os.pathsep + os.environ.get("PYTHONPATH", "")}})
        stage = "worker_ready"
        state["stage"] = stage
        write_json(directory / "state.json", state)
        actor = ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)(CalibrationWorker).remote(str(project), str(directory), plan)
        phase = time.monotonic()
        worker = ray.get(actor.ready.remote(), timeout=120)
        provenance["ray_worker_ready_s"] = time.monotonic() - phase
        if (type(worker.get("worker_pid")) is not int or worker["worker_pid"] == os.getpid()
                or worker.get("hostname") != provenance["hostname"]
                or worker.get("source_sha256") != plan["runtime_sha256"]):
            raise RuntimeError("Worker identity or source verification failed")
        provenance["worker"] = worker
        write_json(directory / "provenance.json", provenance)
        for record in state["attempts"]:
            if launcher.poll() is not None:
                raise RuntimeError("Owned Ray launcher exited before the next trial")
            stage = record["trial_id"]
            state["stage"] = stage
            record["status"] = "running"
            write_json(directory / "state.json", state)
            phase = time.perf_counter()
            try:
                result = ray.get(actor.execute.remote(record["trial_id"]), timeout=TRIAL_TIMEOUT + 60)
                record["attempt_wait_elapsed_s"] = time.perf_counter() - phase
                record.update(result)
                if record["status"] != "success" or record.get("worker_pid") != worker["worker_pid"]:
                    raise RuntimeError("Worker failed; inspect the saved trial/launcher.log")
                checked = validate_benchmark(directory / record["trial_id"], record["configuration"], workload=C20_WORKLOAD)
                if any(record.get(k) != checked[k] for k in ("aggregate", "artifact_sha256")):
                    raise RuntimeError("Worker result differs from raw evidence")
            except BaseException as exc:
                if record["attempt_wait_elapsed_s"] is None:
                    record["attempt_wait_elapsed_s"] = time.perf_counter() - phase
                record["status"] = "failed" if isinstance(exc, Exception) else "interrupted"
                record.setdefault("error_type", type(exc).__name__)
                raise  # Stop: a timed-out Ray call can still have remote work.
            finally:
                write_json(directory / "state.json", state)
            print(record["trial_id"], "validated success; harness seconds:", record["aggregate"]["experiment_elapsed_s"], flush=True)
    except BaseException as exc:
        state["fatal_error_type"] = type(exc).__name__
        state["fatal_error_stage"] = stage
        diagnostic_error = save_exception(directory / "failure.txt", exc)
        if diagnostic_error:
            state["diagnostic_error_type"] = diagnostic_error
    finally:
        try:
            for label, action in (("actor", lambda: ray.kill(actor) if actor is not None else None),
                                  ("driver", ray.shutdown),
                                  ("launcher", lambda: ray_startup.stop_owned_group(launcher))):
                try:
                    action()
                except BaseException as exc:
                    state["cleanup_error_type"] = type(exc).__name__
                    state.setdefault("cleanup_errors", []).append({"component": label, "error_type": type(exc).__name__})
                    save_exception(directory / ("cleanup-" + label + "-failure.txt"), exc)
        finally:
            try:
                diagnostics = preserve_ray_logs(ray_temp, directory)
                write_json(directory / "ray-diagnostics.json", diagnostics)
                if diagnostics["errors"]:
                    state["ray_log_copy_errors"] = diagnostics["errors"]
            except OSError as exc:
                state["ray_log_copy_error_type"] = type(exc).__name__
            passed = (not any(k in state for k in ("fatal_error_type", "cleanup_error_type"))
                      and all(r["status"] == "success" for r in state["attempts"]))
            state["validation"] = {
                "passed": passed, "planned_trials": 2,
                "attempted_trials": sum(r["status"] != "not_started" for r in state["attempts"]),
                "successful_trials": sum(r["status"] == "success" for r in state["attempts"]),
                "session_including_cleanup_s": time.monotonic() - started,
            }
            write_json(directory / "state.json", state)
            write_json(directory / "validation.json", state["validation"])
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
    print("PASS" if passed else "INCOMPLETE/FAILED", directory, flush=True)
    return directory, passed


def positive(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def audit(directory):
    directory = Path(directory)
    plan = json.loads((directory / "plan.json").read_text())
    validate_plan(plan)
    state = json.loads((directory / "state.json").read_text())
    provenance = json.loads((directory / "provenance.json").read_text())
    errors, rows = [], []
    try:
        verify_inputs(plan, directory / "source-snapshot")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append("source snapshot: " + str(exc))
    worker = provenance.get("worker") or {}
    if (state.get("plan_sha256") != plan["plan_sha256"]
            or provenance.get("source_sha256") != plan["runtime_sha256"]
            or provenance.get("resolved_image_id") not in (plan["image_id"], "id:" + plan["image_id"])
            or not str(provenance.get("job_id", "")).isdigit()
            or not str(provenance.get("hostname", "")).startswith("nid")
            or worker.get("hostname") != provenance.get("hostname")
            or worker.get("source_sha256") != plan["runtime_sha256"]
            or type(provenance.get("driver_pid")) is not int or type(worker.get("worker_pid")) is not int
            or worker.get("worker_pid") == provenance.get("driver_pid")):
        errors.append("missing or inconsistent plan, image, compute node, or worker identity")
    attempts = state.get("attempts", [])
    if [r.get("trial_id") for r in attempts] != [t["trial_id"] for t in plan["trials"]]:
        errors.append("missing, duplicate, or reordered trial records")
    by_id = {r.get("trial_id"): r for r in attempts}
    for trial in plan["trials"]:
        record = by_id.get(trial["trial_id"], {})
        row = {"trial_id": trial["trial_id"], "status": record.get("status", "not_started"),
               "experiment_elapsed_s": None, "energy_hartree": None,
               "worker_elapsed_s": record.get("worker_elapsed_s"),
               "attempt_wait_elapsed_s": record.get("attempt_wait_elapsed_s")}
        try:
            if (record.get("status") != "success" or any(record.get(k) != v for k, v in trial.items())
                    or record.get("worker_pid") != worker.get("worker_pid")):
                raise ValueError("trial incomplete, failed, or inconsistent with plan/worker")
            checked = validate_benchmark(directory / trial["trial_id"], trial["configuration"], workload=C20_WORKLOAD)
            if any(record.get(k) != checked[k] for k in ("aggregate", "artifact_sha256")):
                raise ValueError("raw evidence differs from saved result or hashes")
            times = [row["attempt_wait_elapsed_s"], row["worker_elapsed_s"], checked["aggregate"]["experiment_elapsed_s"]]
            if not all(positive(t) for t in times) or times[2] > times[1] + .001 or times[1] > times[0] + .001:
                raise ValueError("invalid or inconsistent timing boundaries")
            measurement = json.loads((directory / trial["trial_id"] / "measurement.json").read_text())
            row.update(experiment_elapsed_s=times[2], energy_hartree=measurement["cases"][0]["energy_hartree"])
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            errors.append(trial["trial_id"] + ": " + str(exc))
        rows.append(row)
    validation = json.loads((directory / "validation.json").read_text())
    if (validation != state.get("validation") or validation.get("passed") is not True
            or any(validation.get(k) != 2 for k in ("planned_trials", "attempted_trials", "successful_trials"))
            or not positive(validation.get("session_including_cleanup_s"))
            or "fatal_error_type" in state or "cleanup_error_type" in state):
        errors.append("completion report is incomplete or inconsistent")
    values = [r["experiment_elapsed_s"] for r in rows]
    return {"passed": not errors, "job_id": provenance.get("job_id"), "plan_sha256": plan["plan_sha256"],
            "workload": C20_WORKLOAD, "trials": rows, "errors": errors,
            "mean_single_case_harness_s": statistics.mean(values) if not errors else None,
            "sample_sd_single_case_harness_s": statistics.stdev(values) if not errors else None,
            "note": "Two technical calibration runs; no discarded warmup, policy comparison, or measured decision-loop overhead."}


def preflight():
    import ray
    from ray import cloudpickle
    if ray.is_initialized():
        raise ValueError("Run preflight in a fresh Python process")
    _, forwarding = ray_startup.preflight()
    plan = build_plan(PROJECT, (PROJECT / "provenance/nwchem-image-id.txt").read_text().strip())
    ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)(CalibrationWorker)
    worker = CalibrationWorker(str(PROJECT), str(PROJECT / "results"), plan)
    restored = cloudpickle.loads(cloudpickle.dumps(worker))
    if restored.plan != plan or restored.project != PROJECT or ray.is_initialized():
        raise ValueError("Serialization or no-worker preflight invariant failed")
    print("PASS: C20 reference, input, Ray options and serialization; no worker, workload or API request launched")
    print(json.dumps({"python": platform.python_version(), "ray": ray.__version__,
                      "planned_trials": 2, "trial_timeout_s": TRIAL_TIMEOUT,
                      "port_forwarding": forwarding}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("preflight")
    create = sub.add_parser("plan")
    create.add_argument("--out", type=Path, required=True)
    for name in ("check", "submit", "run"):
        p = sub.add_parser(name)
        p.add_argument("--plan", type=Path, required=True)
        p.add_argument("--results-root", type=Path, default=Path("results"))
    inspect = sub.add_parser("audit")
    inspect.add_argument("--session", type=Path, required=True)
    inspect.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "preflight":
            preflight()
        elif args.command == "plan":
            plan = build_plan(PROJECT, (PROJECT / "provenance/nwchem-image-id.txt").read_text().strip())
            args.out.parent.mkdir(parents=True, exist_ok=True)
            with args.out.open("x") as stream:
                json.dump(plan, stream, indent=2, allow_nan=False)
                stream.write("\n")
            print("Frozen calibration: two single-case trials, no discarded warmup; 600s limit per trial")
            print("SHA256:", plan["plan_sha256"])
        elif args.command == "audit":
            report = audit(args.session)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            write_json(args.out, report)
            print(json.dumps(report, indent=2))
            if not report["passed"]:
                raise SystemExit(1)
        else:
            plan = json.loads(args.plan.read_text())
            verify_inputs(plan, PROJECT)
            if args.command == "check":
                print("PASS: frozen plan, current sources, input and upstream reference match")
            else:
                require_committed(args.plan)
                if args.command == "submit":
                    args.results_root.mkdir(parents=True, exist_ok=True)
                    root = args.results_root.resolve()
                    subprocess.run(["sbatch", "--image=id:" + plan["image_id"],
                                    "--output=" + str(root / "c20-calibration-%j.log"),
                                    "--error=" + str(root / "c20-calibration-%j.err"),
                                    str(PROJECT / "scripts/run_workload_calibration.sbatch"),
                                    str(args.plan.resolve()), str(root)], cwd=PROJECT, check=True)
                else:
                    _, passed = run_study(plan, args.results_root)
                    if not passed:
                        raise SystemExit(1)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"ERROR: {exc}\n")


if __name__ == "__main__":
    main()
