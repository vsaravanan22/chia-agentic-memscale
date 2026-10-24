#!/usr/bin/env python3
"""Process-local Ray 2.54.0 dashboard HTTP-port forwarding correction.

The upstream start_raylet() sends --listen-port to dashboard_agent_command,
but omits --dashboard_agent_listen_port from the raylet's own arguments.
This helper supplies that one missing argument and verifies all four agent
ports. It never edits the installed package or fabricates readiness files.
"""
import argparse
from contextlib import contextmanager
import hashlib
import importlib
import json
import os
from pathlib import Path
import shlex
import socket
import sys
from types import SimpleNamespace
from unittest.mock import patch

PORT_MAP = {
    "--dashboard-agent-grpc-port": "metrics_agent_port",
    "--metrics-export-port": "metrics_export_port",
    "--dashboard-agent-listen-port": "dashboard_agent_listen_port",
    "--runtime-env-agent-port": "runtime_env_agent_port",
}


def option(command, name, required=True):
    """Read one generated --name=value argument, accepting dash/underscore aliases."""
    matches = []
    for value in command:
        key, sep, item = value.partition("=")
        if key.removeprefix("--").replace("-", "_") == name:
            if not sep:
                raise ValueError("Expected --name=value: " + name)
            matches.append(item)
    if len(matches) > 1 or (required and not matches):
        raise ValueError("Missing or duplicate option: " + name)
    return matches[0] if matches else None


def expected_ports(cli_args):
    ports = {target: int(option(cli_args, flag[2:].replace("-", "_")))
             for flag, target in PORT_MAP.items()}
    if len(set(ports.values())) != 4 or any(not 1024 <= p <= 65535 for p in ports.values()):
        raise ValueError("Four distinct explicit agent ports are required.")
    return ports


def repair_raylet_command(command, expected):
    if (set(expected) != set(PORT_MAP.values()) or len(set(expected.values())) != 4
            or any(type(p) is not int or not 1024 <= p <= 65535 for p in expected.values())):
        raise ValueError("Invalid expected ports.")
    fixed = list(command)
    dashboard = shlex.split(option(fixed, "dashboard_agent_command"))
    runtime = shlex.split(option(fixed, "runtime_env_agent_command"))
    nested = {
        "metrics_agent_port": int(option(dashboard, "grpc_port")),
        "metrics_export_port": int(option(dashboard, "metrics_export_port")),
        "dashboard_agent_listen_port": int(option(dashboard, "listen_port")),
        "runtime_env_agent_port": int(option(runtime, "runtime_env_agent_port")),
    }
    if nested != expected:
        raise ValueError("Agent commands do not match requested ports.")
    name = "dashboard_agent_listen_port"
    before = option(fixed, name, required=False)
    if before is None:
        fixed.append("--" + name + "=" + str(expected[name]))
    forwarded = {name: int(option(fixed, name)) for name in expected}
    if forwarded != expected:
        raise ValueError("Raylet ports do not match requested ports.")
    return fixed, {"added_missing_dashboard_port": before is None,
                   "agent_ports": nested, "raylet_ports": forwarded}


def services_identity(services):
    path = Path(services.__file__)
    return {"ray_services_path": str(path),
            "ray_services_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


@contextmanager
def forwarded_launches(services, expected, record):
    original = services.start_ray_process

    def launch(command, process_type, *args, **kwargs):
        if process_type == "raylet":
            command, report = repair_raylet_command(command, expected)
            report.update(services_identity(services))
            # Record before dispatch; a worker response is still required for PASS.
            report["meaning"] = "Validated launch arguments; not a readiness result."
            temp = record.with_suffix(".tmp")
            temp.write_text(json.dumps(report, indent=2) + "\n")
            temp.replace(record)
        return original(command, process_type, *args, **kwargs)

    services.start_ray_process = launch
    try:
        yield
    finally:
        services.start_ray_process = original


def preflight():
    """Exercise installed start_raylet's real command builder without launching."""
    ray = importlib.import_module("ray")
    if ray.__version__ != "2.54.0":
        raise RuntimeError("This narrowly scoped correction requires Ray 2.54.0.")
    services = importlib.import_module("ray._private.services")
    expected = dict(zip(PORT_MAP.values(), (24001, 24002, 24003, 24004)))
    captures = []
    sentinel = object()

    def capture(command, process_type, *args, **kwargs):
        if process_type != "raylet":
            raise AssertionError("Unexpected process in preflight.")
        captures.append(list(command))
        return sentinel

    root = "/tmp/chia-port-forwarding-preflight"
    resources = SimpleNamespace(num_cpus=4, labels={}, to_resource_dict=lambda: {"CPU": 4})
    isolation = SimpleNamespace(is_enabled=lambda: False)
    # Intercept dispatch and independently forbid subprocess creation. No Ray init,
    # network request, worker, or scientific workload is part of this check.
    with patch.object(services, "start_ray_process", side_effect=capture), \
            patch("subprocess.Popen", side_effect=AssertionError("No launch in preflight")):
        result = services.start_raylet(
            redis_address="127.0.0.1:24000", gcs_address="127.0.0.1:24000",
            node_id="1" * 56, node_ip_address="127.0.0.1", node_manager_port=0,
            raylet_name=root + "/raylet", plasma_store_name=root + "/plasma",
            cluster_id="2" * 56, worker_path=root + "/worker.py",
            setup_worker_path=root + "/setup.py", temp_dir=root, session_dir=root,
            resource_dir=root + "/resources", log_dir=root + "/logs",
            resource_and_label_spec=resources, plasma_directory="/dev/shm",
            fallback_directory=root, object_store_memory=268435456,
            session_name="forwarding-preflight", is_head_node=True,
            resource_isolation_config=isolation, **expected)
    if result is not sentinel or len(captures) != 1:
        raise RuntimeError("Installed Ray command capture was incomplete.")
    _, report = repair_raylet_command(captures[0], expected)
    report.update(services_identity(services))
    report.update(ray_version=ray.__version__, processes_launched=0)
    return report


def run_cli(cli_args, record):
    cpus = int(os.environ.get("SLURM_CPUS_PER_TASK", "0"))
    tasks = int(os.environ.get("SLURM_NTASKS", "0"))
    if (not os.environ.get("SLURM_JOB_ID", "").isdigit()
            or not socket.gethostname().startswith("nid")
            or not (cpus >= 4 or (tasks >= 8 and cpus >= 2))):
        raise RuntimeError("Use the diagnostic batch script on a Perlmutter CPU node.")
    if not cli_args or cli_args[0] != "start" or not {"--head", "--block"} <= set(cli_args):
        raise ValueError("Only a blocking head-node launch is supported.")
    ray = importlib.import_module("ray")
    if ray.__version__ != "2.54.0":
        raise RuntimeError("Expected Ray 2.54.0.")
    services = importlib.import_module("ray._private.services")
    cli = importlib.import_module("ray.scripts.scripts").cli
    with forwarded_launches(services, expected_ports(cli_args), record):
        cli(args=cli_args, prog_name="ray")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("ray_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    run_cli(args.ray_args, args.record)
