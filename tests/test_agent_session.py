"""Offline control-flow tests. Mock measurements are never research results."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.experiment_session import (
    ExperimentSession, configuration, export_trajectory, run_benchmark,
)
from workload.nwchem_benchmark import run_case


def measurement(throughput=1.0):
    return {"aggregate": {"status": "success", "cases_total": 3, "cases_completed": 3,
                           "throughput_cases_per_s": throughput, "experiment_elapsed_s": 3 / throughput,
                           "latency_mean_ms": 1000, "memory_peak_mb": None},
            "measurement_file": "mock-only.json"}


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        baseline = {"step": 0, "selection_source": "scripted_baseline", "status": "success",
                    "configuration": configuration(1, 1), "receipt": "baseline-receipt",
                    **measurement()}
        prior = [{"configuration": configuration(2, 2), "selection_source": "prior_manual_run",
                  **measurement(1.5)}]
        self.session = ExperimentSession.create(self.directory, self.directory, "test-image", 4,
                                                 baseline, prior)

    def test_history_required(self):
        with patch("tools.experiment_session.run_benchmark") as runner:
            result = self.session.experiment(3, 3, "test", "baseline-receipt")
        self.assertEqual(result["status"], "rejected")
        runner.assert_not_called()

    def test_invalid_and_historical_configs_do_not_launch(self):
        self.session.history()
        with patch("tools.experiment_session.run_benchmark") as runner:
            for executor, batch in [(1, 1), (2, 2), (0, 2), (4, 2), (True, 1), (2, 0), (2, 4)]:
                result = self.session.experiment(executor, batch, "test", "baseline-receipt")
                self.assertEqual(result["status"], "rejected")
        runner.assert_not_called()
        self.assertEqual(self.session.load()["attempts"], [])

    def test_sequential_choices_receipts_budget_and_reports(self):
        history = self.session.history()
        self.assertEqual(len(history["untested_configurations"]), 7)
        receipt = history["latest_receipt"]
        with patch("tools.experiment_session.run_benchmark", side_effect=[
                measurement(0.9), measurement(0.8), measurement(1.2), measurement(1.6)]) as runner:
            for executors, batch in [(1, 2), (2, 1), (2, 3), (3, 3)]:
                result = self.session.experiment(executors, batch, "mock test hypothesis", receipt)
                self.assertEqual(result["status"], "success")
                self.assertEqual(result["observed_receipt"], receipt)
                receipt = result["receipt"]
            rejected = self.session.experiment(3, 2, "over budget", receipt)
            self.assertEqual(rejected["status"], "rejected")
            self.assertEqual(runner.call_count, 4)
        state = self.session.load()
        self.assertEqual([r["configuration"]["partitions"] for r in state["attempts"]], [3] * 4)
        best = json.loads((self.directory / "best_config.json").read_text())
        self.assertEqual(best["best"]["step"], 4)
        self.assertAlmostEqual(best["best_gain_vs_fresh_baseline_pct"], 60)
        events = [json.loads(line)["event"] for line in (self.directory / "events.jsonl").read_text().splitlines()]
        self.assertEqual(events[:3], ["history_read", "experiment_selected", "experiment_finished"])
        self.assertNotIn(b"\r", (self.directory / "trajectory.csv").read_bytes())

    def test_stale_receipt_and_repeat_rejected(self):
        self.session.history()
        with patch("tools.experiment_session.run_benchmark", return_value=measurement()) as runner:
            first = self.session.experiment(1, 2, "test", "baseline-receipt")
            for executor, batch, receipt in [(3, 3, "baseline-receipt"), (1, 2, first["receipt"])]:
                result = self.session.experiment(executor, batch, "test", receipt)
                self.assertEqual(result["status"], "rejected")
            self.assertEqual(runner.call_count, 1)

    def test_failure_persists_consumes_budget_and_agent_can_continue(self):
        self.session.history()
        with patch("tools.experiment_session.run_benchmark", side_effect=RuntimeError("test")):
            failed = self.session.experiment(3, 3, "test", "baseline-receipt")
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(self.session.history()["remaining_budget"], 3)
        with patch("tools.experiment_session.run_benchmark", return_value=measurement()):
            continued = self.session.experiment(2, 3, "recover", failed["receipt"])
        self.assertEqual(continued["status"], "success")
        self.assertEqual(continued["step"], 2)

    def test_worse_experiments_keep_baseline_as_best(self):
        self.session.history()
        with patch("tools.experiment_session.run_benchmark", return_value=measurement(0.5)):
            self.session.experiment(3, 3, "test", "baseline-receipt")
        best = json.loads((self.directory / "best_config.json").read_text())
        self.assertEqual(best["best"]["selection_source"], "scripted_baseline")
        self.assertEqual(best["best_gain_vs_fresh_baseline_pct"], 0)

    def test_failed_attempt_duration_is_persisted_without_inventing_launcher_time(self):
        self.session.history()
        with patch("tools.experiment_session.time.monotonic", side_effect=[0., 10., 15.]), \
                patch("tools.experiment_session.run_benchmark", side_effect=RuntimeError("failure")):
            result = self.session.experiment(3, 3, "test", "baseline-receipt")
        self.assertEqual(result["attempt_elapsed_s"], 5.)
        self.assertNotIn("launcher_elapsed_s", result)
        self.assertEqual(self.session.load()["attempts"][0]["attempt_elapsed_s"], 5.)

    def test_interrupt_persists_attempt_time_and_consumed_budget(self):
        self.session.history()
        with patch("tools.experiment_session.time.monotonic", side_effect=[0., 10., 13.]), \
                patch("tools.experiment_session.run_benchmark", side_effect=KeyboardInterrupt()):
            with self.assertRaises(KeyboardInterrupt):
                self.session.experiment(3, 3, "test", "baseline-receipt")
        record = self.session.load()["attempts"][0]
        self.assertEqual((record["status"], record["attempt_elapsed_s"]), ("interrupted", 3.))
        self.assertEqual(self.session.history()["remaining_budget"], 3)

    def test_real_runner_rejects_incorrect_scientific_energy(self):
        def fake_child(command, **kwargs):
            data = {"configuration": configuration(3, 3), **measurement(),
                    "cases": [{"case_id": f"p{i}-r0", "passed": True, "returncode": 0,
                               "energy_hartree": 0.0} for i in range(3)]}
            Path(command[-1]).write_text(json.dumps(data))
            from unittest.mock import Mock
            return Mock(wait=Mock(return_value=0))
        with patch("tools.experiment_session.subprocess.Popen", side_effect=fake_child):
            with self.assertRaisesRegex(RuntimeError, "energy validation"):
                run_benchmark(self.directory, self.directory / "invalid", "image", configuration(3, 3))

    def test_harness_requests_exact_resources_and_explicit_image(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        input_path = self.directory / "water.nw"
        input_path.write_text("mock input")
        args = SimpleNamespace(input=input_path, mpi_tasks=2, cpus_per_task=2, image="test/image:tag")
        def fake_process(command, **kwargs):
            kwargs["stdout"].write(b"Total SCF energy = -75.9839975704\n")
            return Mock(returncode=0)
        with patch("workload.nwchem_benchmark.subprocess.run", side_effect=fake_process) as launched:
            case = run_case("p0-r0", 0, 0, args, self.directory)
        command = launched.call_args.args[0]
        self.assertIn("--exclusive", command)
        self.assertIn("--exact", command)
        self.assertEqual(command[command.index("--image") + 1], args.image)
        self.assertEqual(command[command.index("--ntasks") + 1], "2")
        self.assertTrue(case["passed"])


if __name__ == "__main__":
    unittest.main()
