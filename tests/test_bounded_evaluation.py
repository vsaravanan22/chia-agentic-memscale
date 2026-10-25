"""Offline protocol/audit tests; all generated measurements are synthetic."""
import contextlib
import copy
import io
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import run_bounded_evaluation as runner
from scripts.analyze_bounded_evaluation import audit_study, landscape, overhead, retrospective_regret
from scripts.run_policy_confirmation import plan_digest
from scripts.summarize_evaluation_blocks import blocked_comparison, confirmation_rows
from tools.experiment_session import configuration, validate_benchmark, write_json
from test_analyze_policy_comparison import fixture, write
from test_contract_injections import raw_fixture


def synthetic_study(root, kind="landscape", repeats=2):
    root.mkdir()
    plan = runner.build_plan(runner.PROJECT, kind, repeats, 12, "a" * 64)
    write_json(root / "plan.json", plan)
    for name in runner.RUNTIME_FILES:
        target = root / "source-snapshot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(runner.PROJECT / name, target)
    write_json(root / "provenance.json", {"job_id": "123", "hostname": "nid-synthetic",
        "driver_pid": 100, "resolved_image_id": "a" * 64, "source_sha256": plan["runtime_sha256"]})
    raw_fixture(root / "warmup", configuration(1, 1))
    warm = validate_benchmark(root / "warmup", configuration(1, 1))
    attempts = []
    for trial in plan["trials"]:
        directory = runner.measurement_directory(root, trial)
        rate = trial["configuration"]["executors"] * .3 + trial["configuration"]["batch_size"] * .03 + trial["repeat"] * .001
        raw_fixture(directory, trial["configuration"], rate)
        checked = validate_benchmark(directory, trial["configuration"])
        attempts.append({**trial, **checked, "status": "success", "worker_pid": 200,
                         "worker_elapsed_s": 4., "ray_roundtrip_s": 4.1})
        if trial["arm"] == "contract":
            components = {k: .001 for k in ("admission_including_lock", "reservation_writes", "completion_writes", "trajectory_export", "validation", "hashing")}
            attempt = {"step": 1, "status": "success", "configuration": trial["configuration"],
                       "observed_receipt": "baseline", "receipt": "result", **checked}
            write_json(directory.parent / "state.json", {"baseline": {"receipt": "baseline"}, "attempts": [attempt]})
            events = [{"event": "experiment_finished", **attempt},
                      {"event": "contract_profile", "step": 1, "status": "success", "components_s": components}]
            (directory.parent / "events.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")
    validation = {"passed": True, "planned_trials": len(attempts), "attempted_trials": len(attempts), "successful_trials": len(attempts)}
    write_json(root / "state.json", {"plan_sha256": plan["plan_sha256"], "attempts": attempts, "warmup": warm, "validation": validation})
    write_json(root / "validation.json", validation)
    return plan


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_landscape_all_nine_once_per_block_and_deterministic(self):
        plan = runner.build_plan(runner.PROJECT, "landscape", 5, 10, "a" * 64)
        self.assertEqual(len(plan["trials"]), 45)
        for repeat in range(1, 6):
            block = [t for t in plan["trials"] if t["repeat"] == repeat]
            self.assertEqual({(t["configuration"]["executors"], t["configuration"]["batch_size"]) for t in block},
                             {(e, b) for e in (1, 2, 3) for b in (1, 2, 3)})
        self.assertEqual(plan["trials"], runner.schedule("landscape", 5, 10))
        changed = copy.deepcopy(plan)
        changed["trials"][0]["configuration"]["executors"] = 10
        self.assertEqual(plan["trials"], runner.schedule("landscape", 5, 10))

    def test_resigned_missing_duplicate_and_wrong_typed_trials_are_rejected(self):
        plan = runner.build_plan(runner.PROJECT, "landscape", 5, 10, "a" * 64)
        for change in (lambda p: p["trials"].pop(),
                       lambda p: p["trials"].__setitem__(0, p["trials"][1]),
                       lambda p: p["trials"][0]["configuration"].update(executors=True)):
            broken = copy.deepcopy(plan)
            change(broken)
            broken["plan_sha256"] = plan_digest(broken)
            with self.assertRaises(ValueError):
                runner.validate_plan(broken)

    def test_changed_runtime_and_invalid_plan_parameters_rejected(self):
        plan = runner.build_plan(runner.PROJECT, "landscape", 5, 10, "a" * 64)
        plan["runtime_sha256"][runner.RUNTIME_FILES[0]] = "0" * 64
        plan["plan_sha256"] = plan_digest(plan)
        with self.assertRaisesRegex(ValueError, "Runtime changed"):
            runner.verify_inputs(plan, runner.PROJECT)
        for repeats in (0, 1, 21, True, 2.0):
            with self.assertRaises(ValueError):
                runner.build_plan(runner.PROJECT, "landscape", repeats, 10, "a" * 64)

    def test_plan_creation_refuses_overwrite(self):
        out = self.root / "plan.json"
        args = ["run_bounded_evaluation.py", "plan", "--kind", "landscape", "--out", str(out)]
        with patch("sys.argv", args), contextlib.redirect_stdout(io.StringIO()):
            runner.main()
            original = out.read_bytes()
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                runner.main()
        self.assertEqual(out.read_bytes(), original)

    def test_submit_uses_pinned_image_and_explicit_paths(self):
        plan = runner.build_plan(runner.PROJECT, "landscape", 5, 10, "a" * 64)
        path = self.root / "plan.json"
        write_json(path, plan)
        with patch("sys.argv", ["runner", "submit", "--plan", str(path), "--results-root", str(self.root / "results")]), \
                patch.object(runner.subprocess, "run") as submit:
            runner.main()
        cmd = submit.call_args.args[0]
        self.assertIn("--image=id:" + "a" * 64, cmd)
        self.assertEqual(cmd[-2:], [str(path.resolve()), str((self.root / "results").resolve())])

    def test_audit_ranks_every_config_and_bootstraps_repeat_blocks(self):
        plan = synthetic_study(self.root / "study")
        _, rows, errors = audit_study(self.root / "study")
        self.assertEqual(errors, [])
        ranked = landscape(rows, plan["repeats"], bootstrap_samples=100)
        self.assertEqual(ranked[0]["configuration"], "E3/B3")
        self.assertEqual(ranked[0]["empirical_regret_pct"], 0)
        self.assertEqual(ranked[0]["bootstrap_top_fraction"], 1)
        self.assertTrue(all(r["sample_sd"] > 0 for r in ranked))
        self.assertAlmostEqual(sum(r["bootstrap_top_fraction"] for r in ranked), 1.)

    def test_edit_raw_output_even_with_correct_energy_is_hash_mismatch(self):
        synthetic_study(self.root / "study")
        path = next((self.root / "study/trial-0001/cases").rglob("output.txt"))
        path.write_text(path.read_text() + "edited record\n")
        _, _, errors = audit_study(self.root / "study")
        self.assertTrue(any("digest differs" in e for e in errors), errors)

    def test_reordered_attempts_and_edited_aggregate_fail_audit(self):
        synthetic_study(self.root / "study")
        path = self.root / "study/state.json"
        state = json.loads(path.read_text())
        state["attempts"][0], state["attempts"][1] = state["attempts"][1], state["attempts"][0]
        state["attempts"][0]["aggregate"]["throughput_cases_per_s"] = 99
        write_json(path, state)
        _, _, errors = audit_study(self.root / "study")
        self.assertTrue(any("attempt order" in e for e in errors))
        self.assertTrue(any("saved result" in e for e in errors))

    def test_missing_trial_retained_and_ranking_refused(self):
        plan = synthetic_study(self.root / "study")
        path = self.root / "study/state.json"
        state = json.loads(path.read_text())
        state["attempts"].pop()
        write_json(path, state)
        _, rows, errors = audit_study(self.root / "study")
        self.assertEqual(rows[-1]["status"], "not_started")
        self.assertTrue(errors)
        with self.assertRaises(ValueError):
            landscape(rows, plan["repeats"])

    def test_regret_maps_configuration_not_historical_throughput(self):
        directory = fixture(self.root, rates=(.8, 1.2, 1.5), job="11")
        reference = [{"configuration": f"E{e}/B{b}", "mean": 2. if (e, b) == (2, 2) else 1.}
                     for e in (1, 2, 3) for b in (1, 2, 3)]
        rows = retrospective_regret(reference, self.root, ["11"])
        self.assertEqual(rows[0]["recommended_configuration"], "E3/B3")
        self.assertEqual(rows[0]["reference_regret_pct"], 50)
        self.assertEqual(rows[0]["best_visited_reference_regret_pct"], 0)
        self.assertTrue(rows[0]["visited_top_including_baseline"])
        self.assertFalse(rows[0]["recommended_top"])

    def test_overhead_pairs_and_no_negative_difference_clamping(self):
        plan = synthetic_study(self.root / "study", kind="overhead", repeats=4)
        first_arms = [t["arm"] for t in plan["trials"] if int(t["trial_id"].split("-")[1]) % 2]
        self.assertEqual(first_arms.count("bare"), 2)
        _, rows, errors = audit_study(self.root / "study")
        self.assertEqual(errors, [])
        for row in rows:
            row["ray_roundtrip_s"] = 4 if row["arm"] == "contract" else 5
        pairs = overhead(rows)
        self.assertAlmostEqual(pairs[0]["roundtrip_difference_pct"], -20)
        self.assertGreater(pairs[0]["instrumented_components_s"], 0)

    def test_blocks_keep_jobs_separate_and_expose_missing_pairs(self):
        for job in ("1", "2"):
            fixture(self.root, job=job, rates=(1.1, 1.2, 1.3))
            fixture(self.root, job=job, policy="random", rates=(1.1, 1.2, 1.4 if job == "1" else 1.25))
        _, pairs, report = blocked_comparison(self.root, ["1", "2"])
        self.assertTrue(report["audit_passed"])
        self.assertEqual(report["descriptive_total"], {"eligible_pairs": 2, "gemini_wins": 1, "random_wins": 1, "ties": 0})
        self.assertEqual(len(report["blocks"]), 2)
        shutil.rmtree(self.root / "comparison-random-2-101")
        _, _, report = blocked_comparison(self.root, ["1", "2"])
        self.assertFalse(report["audit_passed"])

    def test_login_node_rejected_before_ray_import(self):
        plan = runner.build_plan(runner.PROJECT, "landscape", 2, 10, "a" * 64)
        with patch.dict(os.environ, {}, clear=True), patch("scripts.run_policy_confirmation.socket.gethostname", return_value="login06"):
            with self.assertRaises(RuntimeError):
                runner.run_study(plan, self.root)

    def exercise_driver(self, image_failure=False, interrupted=False, kind="landscape"):
        plan = runner.build_plan(runner.PROJECT, kind, 2, 10, "a" * 64)
        def fake_benchmark(project, directory, image, config, **kwargs):
            raw_fixture(Path(directory), config)
            return {**validate_benchmark(directory, config), "launcher_elapsed_s": .01}
        def fake_launch(project, directory, image, config, timeout_s=120, backend="perlmutter"):
            raw_fixture(Path(directory), config)
            return {"measurement_file": str(Path(directory) / "measurement.json"), "launcher_elapsed_s": .01}
        class Actor:
            def __init__(self, worker):
                self.worker = worker
                self.warmup = SimpleNamespace(remote=worker.warmup)
                self.prepare = SimpleNamespace(remote=worker.prepare)
                self.execute = SimpleNamespace(remote=self.execute_trial)
            def execute_trial(self, trial):
                if interrupted:
                    raise KeyboardInterrupt()
                result = self.worker.execute(trial)
                result["worker_pid"] = os.getpid() + 1
                return result
        ray = SimpleNamespace(__version__="synthetic", init=Mock(), shutdown=Mock(), kill=Mock(),
            remote=lambda **kwargs: lambda cls: SimpleNamespace(remote=lambda *args: Actor(cls(*args))),
            get=lambda result, **kwargs: result)
        with patch.dict("sys.modules", {"ray": ray}), \
                patch.dict(os.environ, {"SLURM_JOB_ID": "99999", "SLURM_NTASKS": "8", "SLURM_CPUS_PER_TASK": "2"}), \
                patch("scripts.run_policy_confirmation.socket.gethostname", return_value="nid-synthetic"), \
                patch.object(runner, "verify_image", side_effect=RuntimeError("image mismatch") if image_failure else None, return_value="a" * 64), \
                patch.object(runner, "run_benchmark", side_effect=fake_benchmark), \
                patch.object(runner, "launch_benchmark", side_effect=fake_launch), \
                patch("tools.experiment_session.launch_benchmark", side_effect=fake_launch), \
                contextlib.redirect_stdout(io.StringIO()):
            directory, passed = runner.run_study(plan, self.root)
        return directory, passed, ray

    def test_driver_records_every_trial_and_auditable_cost_components(self):
        for kind in ("landscape", "overhead"):
            directory, passed, _ = self.exercise_driver(kind=kind)
            self.assertTrue(passed)
            _, _, errors = audit_study(directory)
            self.assertEqual(errors, [])

    def test_driver_image_failure_never_starts_ray(self):
        directory, passed, ray = self.exercise_driver(image_failure=True)
        self.assertFalse(passed)
        ray.init.assert_not_called()
        state = json.loads((directory / "state.json").read_text())
        self.assertEqual(state["attempts"], [])

    def test_interrupt_records_partial_results_and_unstarted_schedule(self):
        directory, passed, _ = self.exercise_driver(interrupted=True)
        self.assertFalse(passed)
        _, rows, errors = audit_study(directory)
        self.assertEqual(rows[0]["status"], "interrupted")
        self.assertEqual(sum(r["status"] == "not_started" for r in rows), 17)
        self.assertTrue(errors)


if __name__ == "__main__":
    unittest.main()
