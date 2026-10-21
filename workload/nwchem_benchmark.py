#!/usr/bin/env python3
import argparse
import concurrent.futures
import hashlib
import json
import math
import queue
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

ENERGY = re.compile(r"Total SCF energy\s*=\s*([-+0-9.EeDd]+)")
RSS = re.compile(r"MAXRSS_KB=(\d+)")
REFERENCE = -75.983998
TOLERANCE = 1e-5
WATER_WORKLOAD = "nwchem_water_6-31g_scf"
C20_WORKLOAD = "nwchem_c20_bowl_cc-pvdz_rhf"
WORKLOADS = {
    WATER_WORKLOAD: {"input": "water_check.nw", "reference": REFERENCE,
                     "tolerance": TOLERANCE, "input_sha256": None},
    C20_WORKLOAD: {"input": "c20_bowl_scf.nw", "reference": -756.702486554389,
                   "tolerance": 1e-6,
                   "input_sha256": "5e570e4160ab87ccc31cc82171755cdc2635ce086d17512a77339cc2e5cea4bb"},
}


def workload_definition(name):
    """Closed scientific references, never supplied by an agent or measurement."""
    if name not in WORKLOADS:
        raise ValueError("Unknown scientific workload")
    return dict(WORKLOADS[name])


def verify_workload_input(path, workload):
    expected = workload_definition(workload)["input_sha256"]
    if expected and hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
        raise ValueError("Input differs from the frozen scientific workload")


def c20_output_complete(output):
    """Require the expected SCF problem and a normal end, not an energy alone."""
    return (len(ENERGY.findall(output)) == 1 and all(re.search(pattern, output) for pattern in (
        r"(?m)^\s*functions\s*=\s*280\s*$", r"(?m)^\s*atoms\s*=\s*20\s*$",
        r"(?m)^\s*wavefunction\s*=\s*RHF\s*$", r"Final RHF\s+results",
        r"Total times\s+cpu:\s*[0-9.]+s\s+wall:\s*[0-9.]+s")))


def native_cpu_slots(workers, ranks):
    """Allocate disjoint physical cores, leaving two cores for CHIA/Ray."""
    cores = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        identity = ((topology / "physical_package_id").read_text().strip(),
                    (topology / "core_id").read_text().strip())
        cores.setdefault(identity, cpu)
    selected = list(cores.values())
    if len(selected) < workers * ranks + 2:
        raise ValueError("Native backend needs two spare physical cores plus two per active case")
    slots = queue.Queue()
    for offset in range(0, workers * ranks, ranks):
        slots.put(selected[offset:offset + ranks])
    return slots


def run_case(case_id, partition, replica, args, scratch):
    workload = getattr(args, "workload", WATER_WORKLOAD)
    definition = workload_definition(workload)
    directory = Path(tempfile.mkdtemp(
        prefix=f"nwchem-{case_id}-",
        dir=scratch,
    ))
    shutil.copy2(args.input, directory / args.input.name)
    verify_workload_input(directory / args.input.name, workload)

    command = [
        "srun", "--exclusive", "--exact", "--nodes=1",
        "--ntasks", str(args.mpi_tasks),
        "--cpus-per-task", str(args.cpus_per_task),
        "--cpu-bind=cores",
        "--kill-on-bad-exit=1",
        "shifter", "--image", args.image, "--module=mpich",
        "nwchem", args.input.name,
    ]

    cpu_ids = None
    if getattr(args, "backend", "perlmutter") == "native":
        cpu_ids = args.cpu_slots.get()
        command = ["taskset", "--cpu-list", ",".join(map(str, cpu_ids)),
                   "mpirun", "--bind-to", "none", "-np", str(args.mpi_tasks),
                   args.nwchem, args.input.name]
    environment = os.environ.copy()
    if cpu_ids is not None:
        environment.update(OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1")
    start = time.perf_counter()
    try:
        result = subprocess.run(command, cwd=directory, capture_output=True,
                                text=True, env=environment)
    finally:
        if cpu_ids is not None:
            args.cpu_slots.put(cpu_ids)
    latency_ms = (time.perf_counter() - start) * 1000

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    (directory / "output.txt").write_text(output)

    energies = ENERGY.findall(output)
    energy = None
    if energies:
        energy = float(energies[-1].replace("D", "E").replace("d", "e"))

    rss_values = [int(x) for x in RSS.findall(output)]
    memory_mb = max(rss_values) / 1024 if rss_values else None

    passed = (
        result.returncode == 0
        and energy is not None
        and math.isfinite(energy)
        and abs(energy - definition["reference"]) < definition["tolerance"]
        and (workload != C20_WORKLOAD or c20_output_complete(output))
    )

    return {
        "command": command,
        "cpu_ids": cpu_ids,
        "case_id": case_id,
        "partition": partition,
        "replica": replica,
        "returncode": result.returncode,
        "latency_ms": round(latency_ms, 3),
        "energy_hartree": energy,
        "memory_mb": round(memory_mb, 3) if memory_mb else None,
        "passed": passed,
        "run_directory": str(directory),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--workload", choices=tuple(WORKLOADS), default=WATER_WORKLOAD)
    parser.add_argument("--image", default="unused-native")
    parser.add_argument("--backend", choices=("perlmutter", "native"), default="perlmutter")
    parser.add_argument("--nwchem", default="/usr/bin/nwchem.openmpi")
    parser.add_argument("--scratch-root", type=Path)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--executors", type=int, required=True)
    parser.add_argument("--replication", type=int, required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--placement", default="single-node")
    parser.add_argument("--mpi-tasks", type=int, default=2)
    parser.add_argument("--cpus-per-task", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    args.input = args.input.resolve()
    if not args.input.exists():
        parser.error(f"Missing input: {args.input}")
    definition = workload_definition(args.workload)
    try:
        verify_workload_input(args.input, args.workload)
    except ValueError as exc:
        parser.error(str(exc))

    if min(args.partitions, args.replication, args.executors, args.batch_size,
           args.mpi_tasks, args.cpus_per_task) < 1:
        parser.error("Counts must be positive")
    if args.backend == "native":
        if args.mpi_tasks != 2 or args.cpus_per_task != 1:
            parser.error("Native experiments use two MPI ranks and one CPU per rank")
        for executable in ("taskset", "mpirun", args.nwchem):
            if shutil.which(executable) is None:
                parser.error(f"Executable missing: {executable}")
        workers = min(args.executors, args.batch_size, args.partitions * args.replication)
        args.cpu_slots = native_cpu_slots(workers, args.mpi_tasks)
    scratch = args.scratch_root or Path(os.environ.get("SCRATCH", tempfile.gettempdir()))
    scratch.mkdir(parents=True, exist_ok=True)

    cases = [
        (f"p{p}-r{r}", p, r)
        for p in range(args.partitions)
        for r in range(args.replication)
    ]

    start = time.perf_counter()
    results = []

    for offset in range(0, len(cases), args.batch_size):
        wave = cases[offset:offset + args.batch_size]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(args.executors, len(wave))
        ) as pool:
            futures = [
                pool.submit(run_case, case_id, p, r, args, scratch)
                for case_id, p, r in wave
            ]
            results.extend(f.result() for f in futures)

    elapsed = time.perf_counter() - start
    successful = [x for x in results if x["passed"]]
    latencies = [x["latency_ms"] for x in successful]
    memories = [
        x["memory_mb"] for x in successful
        if x["memory_mb"] is not None
    ]

    record = {
        "backend": args.backend,
        "workload": args.workload,
        "configuration": {
            "partitions": args.partitions,
            "executors": args.executors,
            "replication": args.replication,
            "batch_size": args.batch_size,
            "placement": args.placement,
        },
        "aggregate": {
            "status": "success"
            if len(successful) == len(results)
            else "failed",
            "cases_completed": len(successful),
            "cases_total": len(results),
            "experiment_elapsed_s": round(elapsed, 3),
            "latency_mean_ms": round(sum(latencies) / len(latencies), 3)
            if latencies else None,
            "throughput_cases_per_s": round(len(successful) / elapsed, 6)
            if elapsed else None,
            "memory_peak_mb": round(max(memories), 3)
            if memories else None,
        },
        "scientific_validation": {
            "reference_energy_hartree": definition["reference"],
            "tolerance_hartree": definition["tolerance"],
        },
        "cases": results,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))

    if record["aggregate"]["status"] != "success":
        raise SystemExit("At least one NWChem case failed")


if __name__ == "__main__":
    main()
