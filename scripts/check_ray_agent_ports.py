#!/usr/bin/env python3
"""Bounded Ray 2.54.0 startup diagnostic; never launches a scientific workload."""
import argparse
from contextlib import ExitStack
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback

if __package__:
    from . import ray_port_forwarding
else:
    import ray_port_forwarding

PROJECT = Path(__file__).resolve().parents[1]
PORT_FLAGS = ("--port", "--dashboard-agent-grpc-port", "--metrics-export-port",
              "--dashboard-agent-listen-port", "--runtime-env-agent-port")
LOG_NAMES = ("raylet.out", "raylet.err", "gcs_server.out", "gcs_server.err",
             "dashboard.log", "dashboard.out", "dashboard.err", "dashboard_agent.log",
             "dashboard_agent.out", "dashboard_agent.err", "runtime_env_agent.log",
             "runtime_env_agent.out", "runtime_env_agent.err", "monitor.log",
             "monitor.out", "monitor.err", "ray_process_exit.log")


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def preflight():
    ray = importlib.import_module("ray")
    if ray.__version__ != "2.54.0":
        raise RuntimeError("This diagnostic requires the installed Ray 2.54.0.")
    start = importlib.import_module("ray.scripts.scripts").start
    available = {option for param in start.params for option in param.opts}
    required = {*PORT_FLAGS, "--head", "--block", "--include-dashboard", "--num-cpus",
                "--num-gpus", "--object-store-memory", "--disable-usage-stats",
                "--temp-dir", "--node-ip-address"}
    if not required <= available:
        raise RuntimeError("Ray CLI is missing options: " + str(sorted(required - available)))
    return ray, ray_port_forwarding.preflight()


def select_ports():
    # Hold all sockets together to avoid selecting the same ephemeral port twice.
    # Release immediately before startup; a subsequent bind conflict is a failure,
    # not permission to connect to another cluster or retry silently.
    with ExitStack() as sockets:
        ports = {}
        for flag in PORT_FLAGS:
            sock = sockets.enter_context(socket.socket())
            sock.bind(("0.0.0.0", 0))
            ports[flag] = sock.getsockname()[1]
        return ports


def cli_command(tmp, ip, ports, record=None):
    if (set(ports) != set(PORT_FLAGS) or len(set(ports.values())) != len(PORT_FLAGS)
            or any(type(p) is not int or not 1024 <= p <= 65535 for p in ports.values())):
        raise ValueError("All five ports must be distinct, explicit, unprivileged integers.")
    record = record or tmp / "port-forwarding.json"
    return [sys.executable, "-u", str(PROJECT / "scripts/ray_port_forwarding.py"),
            "--record", str(record), "start", "--head", "--block",
            "--include-dashboard=false", "--num-cpus=4", "--num-gpus=0",
            "--object-store-memory=268435456", "--disable-usage-stats",
            "--temp-dir=" + str(tmp), "--node-ip-address=" + ip,
            *(flag + "=" + str(ports[flag]) for flag in PORT_FLAGS)]


def wait_for_cli(proc, log, timeout=180, ray_tmp=None):
    deadline = time.monotonic() + timeout
    while True:
        if proc.poll() is not None:
            raise RuntimeError("Ray launcher exited; inspect ray-start.log")
        if ray_tmp is not None:
            for error_log in ray_tmp.glob("session_*/logs/raylet.err"):
                if error_log.is_symlink():
                    continue
                errors = error_log.read_text(errors="replace")
                if "Check failed:" in errors:
                    raise RuntimeError("Raylet fatal error; inspect preserved raylet.err")
        if "Ray runtime started." in log.read_text(errors="replace"):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("Ray launcher did not announce readiness before its deadline")
        time.sleep(0.5)


def wait_for_agents(proc, ip, ports, timeout=90):
    deadline = time.monotonic() + timeout
    pending = {flag: port for flag, port in ports.items() if flag != "--port"}
    while pending:
        if proc.poll() is not None:
            raise RuntimeError("Ray launcher exited while waiting for agents")
        for flag, port in list(pending.items()):
            try:
                with socket.create_connection((ip, port), timeout=0.5):
                    del pending[flag]
            except OSError:
                pass
        if pending and time.monotonic() >= deadline:
            raise TimeoutError("Agents did not listen on ports: " + str(pending))
        if pending:
            time.sleep(0.5)


def stop_owned_group(proc):
    """Stop only the CLI process group created by this diagnostic."""
    if proc is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait(timeout=5)


def copy_logs(tmp, directory):
    result = {"files": [], "errors": []}
    if tmp is None:
        return result
    for session in sorted(tmp.glob("session_*")):
        if session.is_symlink() or not session.is_dir() or (session / "logs").is_symlink():
            continue
        for name in LOG_NAMES:
            source = session / "logs" / name
            if source.is_symlink() or not source.is_file():
                continue
            target = directory / "ray-logs" / session.name / name
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                result["files"].append(str(target.relative_to(directory)))
            except OSError as exc:
                result["errors"].append({"file": name, "error_type": type(exc).__name__})
    return result


def run_probe():
    hostname = socket.gethostname()
    job = os.environ.get("SLURM_JOB_ID", "")
    if (not job.isdigit() or not hostname.startswith("nid")
            or int(os.environ.get("SLURM_CPUS_PER_TASK", "0")) < 4):
        raise RuntimeError("Submit the diagnostic batch script on a Perlmutter CPU node.")
    out = PROJECT / "results" / ("ray-agent-ports-" + job + "-" + str(time.time_ns()))
    out.mkdir(parents=True, exist_ok=False)
    report = {"passed": False, "stage": "preflight", "job_id": job, "hostname": hostname,
              "python": sys.version.split()[0], "registration_wait_s": 120,
              "scientific_workload_launched": False, "cleanup_errors": []}
    for name in ("scripts/check_ray_agent_ports.py", "scripts/check_ray_agent_ports.sbatch",
                 "scripts/ray_port_forwarding.py"):
        target = out / "source-snapshot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        data = (PROJECT / name).read_bytes()
        target.write_bytes(data)
        report.setdefault("source_sha256", {})[name] = hashlib.sha256(data).hexdigest()
    write_json(out / "startup.json", report)
    print("Diagnostic directory:", out, flush=True)
    started, proc, tmp, ray = time.monotonic(), None, None, None
    old_handlers = {}

    def interrupted(signum, frame):
        raise KeyboardInterrupt("signal " + str(signum))

    for signum in (signal.SIGINT, signal.SIGTERM):
        old_handlers[signum] = signal.signal(signum, interrupted)
    try:
        os.environ["RAY_raylet_start_wait_time_s"] = "120"
        ray, forwarding_preflight = preflight()
        report["ray_version"] = ray.__version__
        report["port_forwarding_preflight"] = forwarding_preflight
        ip = importlib.import_module("ray._private.services").get_node_ip_address()
        tmp = Path(tempfile.mkdtemp(prefix="chia-ray-", dir="/tmp"))
        ports = select_ports()
        address = ip + ":" + str(ports["--port"])
        cmd = cli_command(tmp, ip, ports, out / "port-forwarding.json")
        report.update(stage="cli_startup", ports=ports, address=address,
                      command=cmd, ray_temp_dir=str(tmp))
        write_json(out / "startup.json", report)
        log = out / "ray-start.log"
        phase = time.monotonic()
        with log.open("w") as output:
            proc = subprocess.Popen(cmd, stdout=output, stderr=subprocess.STDOUT,
                                    start_new_session=True, cwd=PROJECT)
        wait_for_cli(proc, log, ray_tmp=tmp)
        report["port_forwarding"] = json.loads((out / "port-forwarding.json").read_text())
        report["cli_startup_s"] = time.monotonic() - phase
        report["stage"] = "agent_readiness"
        write_json(out / "startup.json", report)
        print("Ray announced startup; waiting for all four agent ports...", flush=True)
        phase = time.monotonic()
        wait_for_agents(proc, ip, ports)
        report["agent_readiness_s"] = time.monotonic() - phase
        report["stage"] = "worker_probe"
        write_json(out / "startup.json", report)
        print("Agent ports are listening; checking a separate worker...", flush=True)
        phase = time.monotonic()
        ray.init(address=address, runtime_env={"env_vars": {
            "PYTHONPATH": str(PROJECT) + os.pathsep + os.environ.get("PYTHONPATH", "")}})
        probe = ray.remote(num_cpus=1, max_retries=0)(
            lambda: {"pid": os.getpid(), "hostname": socket.gethostname()})
        worker = ray.get(probe.remote(), timeout=90)
        if worker["pid"] == os.getpid() or worker["hostname"] != hostname:
            raise RuntimeError("Unexpected worker identity")
        if proc.poll() is not None:
            raise RuntimeError("Ray launcher exited during the worker check")
        report.update(worker_probe_s=time.monotonic() - phase, worker=worker,
                      passed=True, stage="completed")
    except BaseException as exc:
        report["error_type"] = type(exc).__name__
        detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        print(detail, file=sys.stderr, flush=True)
        try:
            (out / "failure.txt").write_text(detail)
        except OSError as write_error:
            report["failure_write_error"] = type(write_error).__name__
    finally:
        for action in ((lambda: ray.shutdown()) if ray is not None else (lambda: None),
                       lambda: stop_owned_group(proc)):
            try:
                action()
            except BaseException as exc:
                report["cleanup_errors"].append(type(exc).__name__)
                report["passed"] = False
        try:
            diagnostics = copy_logs(tmp, out)
            write_json(out / "ray-diagnostics.json", diagnostics)
            report["log_copy_errors"] = diagnostics["errors"]
        except OSError as exc:
            report["log_copy_error_type"] = type(exc).__name__
        report["session_elapsed_s"] = time.monotonic() - started
        write_json(out / "startup.json", report)
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
    print("PASS" if report["passed"] else "FAIL", out, flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    if args.preflight:
        ray, detail = preflight()
        print("PASS: Ray", ray.__version__, "installed raylet command forwarding; no processes launched")
        print(json.dumps(detail, sort_keys=True))
    else:
        raise SystemExit(run_probe())
