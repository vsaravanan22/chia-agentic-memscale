"""Offline C20/driver tests; synthetic outputs are never performance evidence."""
import contextlib
import copy
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import run_workload_calibration as runner
from tools.experiment_session import configuration, launch_benchmark, validate_benchmark, write_json
from workload.nwchem_benchmark import (C20_WORKLOAD, WATER_WORKLOAD, c20_output_complete,
                                      run_case, workload_definition)
from test_contract_injections import raw_fixture

SYNTHETIC = """Synthetic unit-test output, NOT a scientific measurement.
 functions = 280
 atoms = 20
 wavefunction = RHF
 Final RHF  results
 Total SCF energy = -756.702486554389
 Total times  cpu: 10.0s wall: 10.0s
"""


def fake_process(output):
    def run(command, **kwargs):
        kwargs["stdout"].write(output.encode())
        return Mock(returncode=0)
    return run


def c20_fixture(directory, text=SYNTHETIC):
    case_dir = directory / "cases/nwchem-p0-r0-synthetic"
    case_dir.mkdir(parents=True)
    (case_dir / "output.txt").write_text(text)
    shutil.copyfile(runner.PROJECT / runner.INPUT, case_dir / "c20_bowl_scf.nw")
    definition = workload_definition(C20_WORKLOAD)
    data = {"backend": "perlmutter", "workload": C20_WORKLOAD, "configuration": dict(runner.CONFIG),
            "scientific_validation": {"reference_energy_hartree": definition["reference"],
                                      "tolerance_hartree": definition["tolerance"]},
            "aggregate": {"status": "success", "cases_total": 1, "cases_completed": 1,
                          "throughput_cases_per_s": .1, "experiment_elapsed_s": 10.,
                          "latency_mean_ms": 10000., "memory_peak_mb": None},
            "cases": [{"case_id": "p0-r0", "partition": 0, "replica": 0, "passed": True,
                       "returncode": 0, "energy_hartree": definition["reference"],
                       "run_directory": str(case_dir), "latency_ms": 10000., "memory_mb": None}]}
    write_json(directory / "measurement.json", data)
    return data


def synthetic_study(directory):
    directory.mkdir()
    plan = runner.build_plan(runner.PROJECT, "a" * 64)
    write_json(directory / "plan.json", plan)
    for name in runner.RUNTIME_FILES:
        target = directory / "source-snapshot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(runner.PROJECT / name, target)
    worker = {"worker_pid": 200, "hostname": "nid-synthetic", "source_sha256": plan["runtime_sha256"]}
    write_json(directory / "provenance.json", {"job_id": "123", "hostname": "nid-synthetic", "driver_pid": 100,
               "resolved_image_id": "a" * 64, "source_sha256": plan["runtime_sha256"], "worker": worker})
    records = []
    for trial in plan["trials"]:
        c20_fixture(directory / trial["trial_id"])
        checked = validate_benchmark(directory / trial["trial_id"], trial["configuration"], workload=C20_WORKLOAD)
        records.append({**trial, **checked, "status": "success", "worker_pid": 200,
                        "worker_elapsed_s": 11., "attempt_wait_elapsed_s": 12.})
    validation = {"passed": True, "planned_trials": 2, "attempted_trials": 2,
                  "successful_trials": 2, "session_including_cleanup_s": 25.}
    write_json(directory / "state.json", {"plan_sha256": plan["plan_sha256"], "attempts": records, "validation": validation})
    write_json(directory / "validation.json", validation)
    return plan


class CalibrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_reference_bytes_and_scientific_case_match_upstream(self):
        runner.verify_reference(runner.PROJECT)
        self.assertTrue(c20_output_complete((runner.PROJECT / "provenance/c20-bowl/upstream.out").read_text()))
        for text in (SYNTHETIC.replace("280", "114"), SYNTHETIC.replace("RHF", "UHF"),
                     SYNTHETIC.split("Total times")[0], SYNTHETIC + "Total SCF energy = -756.7\n"):
            self.assertFalse(c20_output_complete(text))

    def test_input_change_and_unknown_workload_rejected_before_launch(self):
        path = self.root / runner.INPUT
        path.parent.mkdir(parents=True)
        path.write_text("task scf energy\n")
        with patch("tools.experiment_session.subprocess.Popen") as launch:
            for workload in (C20_WORKLOAD, "unregistered"):
                with self.assertRaises(ValueError):
                    launch_benchmark(self.root, self.root / "trial", "image", runner.CONFIG, workload=workload)
        launch.assert_not_called()
        self.assertFalse((self.root / "trial").exists())

    def test_c20_launch_uses_registered_input_and_two_rank_resources(self):
        with patch("tools.experiment_session.subprocess.Popen", return_value=Mock(wait=Mock(return_value=0))) as launch:
            launch_benchmark(runner.PROJECT, self.root / "trial", "id:" + "a" * 64,
                             runner.CONFIG, 600, workload=C20_WORKLOAD)
        cmd = launch.call_args.args[0]
        self.assertEqual(cmd[cmd.index("--workload") + 1], C20_WORKLOAD)
        self.assertEqual(cmd[cmd.index("--input") + 1], str(runner.PROJECT / runner.INPUT))
        self.assertEqual(cmd[cmd.index("--mpi-tasks") + 1], "2")
        self.assertEqual(cmd[cmd.index("--cpus-per-task") + 1], "2")
        self.assertEqual(launch.return_value.wait.call_args.kwargs["timeout"], 600)

    def test_timeout_terminates_launcher_process_group(self):
        child = Mock(pid=12345, wait=Mock(side_effect=[subprocess.TimeoutExpired("synthetic", 600), 0]))
        with patch("tools.experiment_session.subprocess.Popen", return_value=child), \
                patch("tools.experiment_session.os.killpg") as kill:
            with self.assertRaises(subprocess.TimeoutExpired):
                launch_benchmark(runner.PROJECT, self.root / "trial", "image", runner.CONFIG, 600, workload=C20_WORKLOAD)
        kill.assert_called_once_with(child.pid, runner.signal.SIGTERM)
        self.assertEqual(child.wait.call_count, 2)

    def test_harness_uses_c20_reference_and_requires_completed_scf(self):
        args = SimpleNamespace(input=runner.PROJECT / runner.INPUT, workload=C20_WORKLOAD,
                               mpi_tasks=2, cpus_per_task=2, image="synthetic")
        for output, passed in ((SYNTHETIC, True), (SYNTHETIC.replace("-756.702486554389", "-75.983998"), False),
                               (SYNTHETIC.split("Total times")[0], False)):
            with patch("workload.nwchem_benchmark.subprocess.run", side_effect=fake_process(output)):
                self.assertEqual(run_case("p0-r0", 0, 0, args, self.root)["passed"], passed)

    def test_harness_cli_exports_c20_measurement_accepted_by_runtime(self):
        from workload.nwchem_benchmark import main
        directory = self.root / "trial"
        # Exercise the real CLI, scheduler, case parser and JSON validator with
        # only the external NWChem process replaced by a synthetic response.
        args = ["benchmark", "--input", str(runner.PROJECT / runner.INPUT), "--workload", C20_WORKLOAD,
                "--partitions", "1", "--executors", "1", "--replication", "1", "--batch-size", "1",
                "--scratch-root", str(directory / "cases"), "--output", str(directory / "measurement.json")]
        with patch("sys.argv", args), contextlib.redirect_stdout(io.StringIO()), \
                patch("workload.nwchem_benchmark.subprocess.run", side_effect=fake_process(SYNTHETIC)), \
                patch("workload.nwchem_benchmark.time.perf_counter", side_effect=[0., 1., 2., 10.]):
            main()
        checked = validate_benchmark(directory, runner.CONFIG, workload=C20_WORKLOAD)
        self.assertEqual(checked["aggregate"]["cases_completed"], 1)

    def test_profiles_cannot_accept_other_workload_or_changed_reference(self):
        directory = self.root / "trial"
        data = c20_fixture(directory)
        checked = validate_benchmark(directory, runner.CONFIG, workload=C20_WORKLOAD)
        self.assertEqual(len(checked["artifact_sha256"]), 3)  # measurement, raw output, input
        with self.assertRaises(RuntimeError):
            validate_benchmark(directory, runner.CONFIG)  # Water still requires three cases.
        for field, value in (("workload", WATER_WORKLOAD), ("scientific_validation", {
                "reference_energy_hartree": 0., "tolerance_hartree": 1000.})):
            changed = {**data, field: value}
            write_json(directory / "measurement.json", changed)
            with self.assertRaises(RuntimeError):
                validate_benchmark(directory, runner.CONFIG, workload=C20_WORKLOAD)
        raw_fixture(self.root / "water", configuration(2, 2))
        self.assertEqual(validate_benchmark(self.root / "water", configuration(2, 2))["aggregate"]["cases_total"], 3)

    def test_raw_input_and_energy_corruption_rejected(self):
        directory = self.root / "trial"
        c20_fixture(directory)
        input_path = directory / "cases/nwchem-p0-r0-synthetic/c20_bowl_scf.nw"
        input_path.write_text(input_path.read_text() + "task tce energy\n")
        with self.assertRaises(ValueError):
            validate_benchmark(directory, runner.CONFIG, workload=C20_WORKLOAD)
        shutil.copyfile(runner.PROJECT / runner.INPUT, input_path)
        output = input_path.with_name("output.txt")
        output.write_text(SYNTHETIC.replace("-756.702486554389", "-756.6"))
        with self.assertRaises(RuntimeError):
            validate_benchmark(directory, runner.CONFIG, workload=C20_WORKLOAD)

    def test_resigned_changed_protocol_and_source_fail(self):
        plan = runner.build_plan(runner.PROJECT, "a" * 64)
        changes = (lambda p: p.update(trial_timeout_s=1200), lambda p: p["trials"].reverse(),
                   lambda p: p["trials"][0]["configuration"].update(partitions=True),
                   lambda p: p.update(reference_energy_hartree=0.), lambda p: p.update(image_id="latest"))
        for change in changes:
            broken = copy.deepcopy(plan)
            change(broken)
            broken["plan_sha256"] = runner.plan_digest(broken)
            with self.assertRaises(ValueError):
                runner.validate_plan(broken)
        plan["runtime_sha256"][runner.INPUT] = "0" * 64
        plan["plan_sha256"] = runner.plan_digest(plan)
        with self.assertRaisesRegex(ValueError, "Source changed"):
            runner.verify_inputs(plan, runner.PROJECT)

    def test_plan_creation_refuses_overwrite(self):
        out = self.root / "plan.json"
        with patch("sys.argv", ["calibration", "plan", "--out", str(out)]), contextlib.redirect_stdout(io.StringIO()):
            runner.main()
            original = out.read_bytes()
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                runner.main()
        self.assertEqual(out.read_bytes(), original)

    def test_uncommitted_code_or_plan_blocks_submission(self):
        path = self.root / "plan.json"
        path.write_text("{}\n")
        missing = SimpleNamespace(returncode=128, stdout=b"")
        with patch.object(runner.subprocess, "run", return_value=missing):
            with self.assertRaisesRegex(ValueError, "Commit the exact"):
                runner.require_committed(path, self.root)

    def test_submit_requires_committed_inputs_and_supplies_pinned_image(self):
        plan_path = self.root / "plan.json"
        write_json(plan_path, runner.build_plan(runner.PROJECT, "a" * 64))
        with patch("sys.argv", ["calibration", "submit", "--plan", str(plan_path), "--results-root", str(self.root)]), \
                patch.object(runner, "require_committed") as committed, patch.object(runner.subprocess, "run") as submit:
            runner.main()
        committed.assert_called_once_with(plan_path)
        cmd = submit.call_args.args[0]
        self.assertIn("--image=id:" + "a" * 64, cmd)
        self.assertEqual(cmd[-2:], [str(plan_path.resolve()), str(self.root.resolve())])

    def test_worker_rejects_repeats_order_and_exhaustion(self):
        plan = runner.build_plan(runner.PROJECT, "a" * 64)
        worker = runner.CalibrationWorker(runner.PROJECT, self.root, plan)
        with patch.object(runner, "run_benchmark", side_effect=RuntimeError("injected")) as launch, \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(ValueError):
                worker.execute("trial-0002")
            self.assertEqual(worker.execute("trial-0001")["status"], "failed")
            with self.assertRaises(ValueError):
                worker.execute("trial-0001")
            self.assertEqual(worker.execute("trial-0002")["status"], "failed")
            with self.assertRaises(ValueError):
                worker.execute("trial-0003")
            self.assertEqual(launch.call_count, 2)

    def test_audit_includes_both_measurements_and_rejects_raw_or_snapshot_edits(self):
        directory = self.root / "study"
        synthetic_study(directory)
        report = runner.audit(directory)
        self.assertTrue(report["passed"], report["errors"])
        self.assertEqual(report["mean_single_case_harness_s"], 10.)
        output = directory / "trial-0001/cases/nwchem-p0-r0-synthetic/output.txt"
        output.write_text(SYNTHETIC + "edited but correct energy\n")
        report = runner.audit(directory)
        self.assertFalse(report["passed"])
        self.assertIsNone(report["mean_single_case_harness_s"])
        output.write_text(SYNTHETIC)
        (directory / "source-snapshot" / runner.INPUT).write_text("changed input\n")
        self.assertFalse(runner.audit(directory)["passed"])

    def test_audit_rejects_reordered_missing_invalid_timing_and_worker(self):
        directory = self.root / "study"
        synthetic_study(directory)
        original = json.loads((directory / "state.json").read_text())
        for change in (lambda s: s["attempts"].reverse(), lambda s: s["attempts"].pop(),
                       lambda s: s["attempts"][0].update(worker_elapsed_s=-1),
                       lambda s: s["attempts"][0].update(worker_pid=100),
                       lambda s: s["attempts"][0].update(attempt_wait_elapsed_s=None)):
            state = copy.deepcopy(original)
            change(state)
            write_json(directory / "state.json", state)
            report = runner.audit(directory)
            self.assertFalse(report["passed"])
            self.assertIsNone(report["mean_single_case_harness_s"])

    def test_login_guard_precedes_ray_import(self):
        plan = runner.build_plan(runner.PROJECT, "a" * 64)
        with patch.dict(os.environ, {}, clear=True), patch("scripts.run_policy_confirmation.socket.gethostname", return_value="login32"):
            with self.assertRaises(RuntimeError):
                runner.run_study(plan, self.root)
        self.assertEqual(list(self.root.iterdir()), [])

    def exercise_driver(self, mode="success"):
        plan = runner.build_plan(runner.PROJECT, "a" * 64)
        launches = Mock()
        make_temp = tempfile.mkdtemp
        def fake_init(**kwargs):
            root = Path(kwargs["_temp_dir"])
            logs = root / "session_synthetic/logs"
            logs.mkdir(parents=True)
            (logs / "raylet.err").write_text("Synthetic Ray startup diagnostic, not performance evidence.\n")
            (logs / "gcs_server.out").write_text("Synthetic GCS diagnostic.\n")
            (root / "session_latest").symlink_to(logs.parent, target_is_directory=True)
            if mode == "ray_startup":
                raise Exception("synthetic node registration failure")
        def fake_benchmark(project, directory, image, config, **kwargs):
            launches()
            if mode == "failure":
                raise subprocess.TimeoutExpired("synthetic", 600)
            c20_fixture(Path(directory))
            return {**validate_benchmark(directory, config, workload=C20_WORKLOAD), "launcher_elapsed_s": 10.}
        class Actor:
            def __init__(self, worker):
                self.worker = worker
                self.ready = SimpleNamespace(remote=self.ready_worker)
                self.execute = SimpleNamespace(remote=self.execute_trial)
            def ready_worker(self):
                return {**self.worker.ready(), "worker_pid": os.getpid() + 1}
            def execute_trial(self, trial):
                if mode == "interrupt":
                    raise KeyboardInterrupt()
                if mode == "ray_timeout":
                    raise TimeoutError("synthetic Ray wait")
                return {**self.worker.execute(trial), "worker_pid": os.getpid() + 1}
        ray = SimpleNamespace(__version__="synthetic", init=Mock(side_effect=fake_init), shutdown=Mock(), kill=Mock(),
            remote=Mock(side_effect=lambda **kwargs: lambda cls: SimpleNamespace(remote=lambda *args: Actor(cls(*args)))),
            get=lambda result, **kwargs: result)
        with patch.dict("sys.modules", {"ray": ray}), \
                patch.dict(os.environ, {"SLURM_JOB_ID": "999", "SLURM_NTASKS": "8", "SLURM_CPUS_PER_TASK": "2"}), \
                patch("scripts.run_policy_confirmation.socket.gethostname", return_value="nid-synthetic"), \
                patch.object(runner, "verify_image", side_effect=RuntimeError("image mismatch") if mode == "image" else None, return_value="a" * 64), \
                patch.object(runner.tempfile, "mkdtemp", side_effect=lambda **kwargs: make_temp(prefix="ray-", dir=self.root)), \
                patch.object(runner, "run_benchmark", side_effect=fake_benchmark), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            directory, passed = runner.run_study(plan, self.root)
        return directory, passed, ray, launches

    def test_driver_completes_two_trials_without_model_calls(self):
        directory, passed, ray, launches = self.exercise_driver()
        self.assertTrue(passed)
        self.assertEqual(launches.call_count, 2)
        state = json.loads((directory / "state.json").read_text())
        self.assertEqual(state["validation"]["successful_trials"], 2)
        ray.kill.assert_called_once()
        self.assertEqual(len(list((directory / "ray-logs").rglob("raylet.err"))), 1)

    def test_startup_failure_preserves_ray_logs_and_traceback_without_launches(self):
        directory, passed, ray, launches = self.exercise_driver("ray_startup")
        self.assertFalse(passed)
        launches.assert_not_called()
        ray.remote.assert_not_called()
        ray.shutdown.assert_called_once()
        state = json.loads((directory / "state.json").read_text())
        self.assertEqual(state["fatal_error_stage"], "ray_startup")
        self.assertEqual(state["validation"]["attempted_trials"], 0)
        self.assertEqual([r["status"] for r in state["attempts"]], ["not_started"] * 2)
        self.assertIn("synthetic node registration failure", (directory / "failure.txt").read_text())
        # Saved paths use the Ray session name directly, without its live sockets.
        self.assertIn("Synthetic Ray", (directory / "ray-logs/session_synthetic/raylet.err").read_text())
        report = json.loads((directory / "ray-diagnostics.json").read_text())
        self.assertEqual(len(report["files"]), 2)
        self.assertEqual(report["errors"], [])
        provenance = json.loads((directory / "provenance.json").read_text())
        self.assertEqual(provenance["ray_temp_dir"], ray.init.call_args.kwargs["_temp_dir"])

    def test_ray_log_copy_failure_keeps_original_failure_and_completion_report(self):
        with patch.object(runner.shutil, "copyfile", side_effect=OSError("synthetic disk failure")):
            directory, passed, ray, launches = self.exercise_driver("ray_startup")
        self.assertFalse(passed)
        ray.shutdown.assert_called_once()
        launches.assert_not_called()
        state = json.loads((directory / "state.json").read_text())
        self.assertEqual(state["fatal_error_stage"], "ray_startup")
        self.assertEqual(len(state["ray_log_copy_errors"]), 2)
        self.assertEqual(state["validation"]["attempted_trials"], 0)
        self.assertFalse(json.loads((directory / "validation.json").read_text())["passed"])

    def test_driver_image_failure_launches_no_worker_or_workload(self):
        directory, passed, ray, launches = self.exercise_driver("image")
        self.assertFalse(passed)
        ray.init.assert_not_called()
        launches.assert_not_called()
        state = json.loads((directory / "state.json").read_text())
        self.assertEqual([r["status"] for r in state["attempts"]], ["not_started"] * 2)

    def test_failure_interrupt_and_unknown_remote_time_stop_remaining_trial(self):
        for mode in ("failure", "interrupt", "ray_timeout"):
            directory, passed, _, launches = self.exercise_driver(mode)
            self.assertFalse(passed)
            state = json.loads((directory / "state.json").read_text())
            self.assertEqual(state["attempts"][1]["status"], "not_started")
            self.assertIn(state["attempts"][0]["status"], ("failed", "interrupted"))
            self.assertGreater(state["attempts"][0]["attempt_wait_elapsed_s"], 0)
            self.assertLessEqual(launches.call_count, 1)
            if mode in ("interrupt", "ray_timeout"):
                self.assertIsNone(state["attempts"][0]["worker_elapsed_s"])
            if mode == "failure":
                self.assertIn("TimeoutExpired", (directory / "trial-0001/failure.txt").read_text())


if __name__ == "__main__":
    unittest.main()
