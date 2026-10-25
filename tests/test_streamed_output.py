"""Real local subprocesses exercise output preservation; no Ray, MPI or NWChem."""
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from scripts.run_workload_calibration import PROJECT, CONFIG
from tools.experiment_session import launch_benchmark
from workload.nwchem_benchmark import C20_WORKLOAD, run_case


class StreamedOutputTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()

    def executable(self, body):
        path = self.bin / "srun"
        path.write_text("#!" + sys.executable + "\n" + body)
        path.chmod(0o700)
        return {"PATH": str(self.bin) + os.pathsep + os.environ.get("PATH", "")}

    def test_real_harness_timeout_retains_emitted_stdout_stderr_and_launch_metadata(self):
        environment = self.executable(
            "import os, time\n"
            "os.write(1, b'SYNTHETIC partial stdout before timeout\\n')\n"
            "os.write(2, b'SYNTHETIC partial stderr before timeout\\n')\n"
            "time.sleep(30)\n")
        directory = self.root / "trial"
        with patch.dict(os.environ, environment):
            with self.assertRaises(subprocess.TimeoutExpired):
                launch_benchmark(PROJECT, directory, "synthetic-image", CONFIG,
                                 timeout_s=3, workload=C20_WORKLOAD)
        outputs = list((directory / "cases").glob("*/output.txt"))
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].read_bytes(),
            b"SYNTHETIC partial stdout before timeout\nSYNTHETIC partial stderr before timeout\n")
        self.assertFalse((directory / "measurement.json").exists())
        launch = json.loads(outputs[0].with_name("launch.json").read_text())
        self.assertEqual(launch["case_id"], "p0-r0")
        self.assertEqual(launch["command"][0], "srun")
        self.assertEqual(launch["output_file"], str(outputs[0]))
        events = [json.loads(line) for line in (directory / "launcher.log").read_text().splitlines()]
        self.assertEqual(events[0]["event"], "harness_launch")
        self.assertEqual(events[-1]["error_type"], "TimeoutExpired")

    def test_completed_case_preserves_raw_bytes_and_parses_energy(self):
        environment = self.executable(
            "import os\n"
            "os.write(1, b'SYNTHETIC stdout \\xff\\n')\n"
            "os.write(2, b'SYNTHETIC stderr \\xfe\\n')\n"
            "os.write(1, b'Total SCF energy = -75.983998\\n')\n")
        source = self.root / "input.nw"
        source.write_text("synthetic input\n")
        args = SimpleNamespace(input=source, mpi_tasks=2, cpus_per_task=2, image="synthetic")
        with patch.dict(os.environ, environment):
            case = run_case("p0-r0", 0, 0, args, self.root)
        self.assertTrue(case["passed"])
        self.assertEqual((Path(case["run_directory"]) / "output.txt").read_bytes(),
                         b"SYNTHETIC stdout \xff\nSYNTHETIC stderr \xfe\nTotal SCF energy = -75.983998\n")

    def test_failed_timeout_logging_still_terminates_process_group(self):
        stream = MagicMock()
        stream.__enter__.return_value = stream
        stream.write.side_effect = [1, OSError("synthetic full filesystem")]
        child = Mock(pid=12345, wait=Mock(side_effect=[subprocess.TimeoutExpired("synthetic", 3), 0]))
        with patch.object(Path, "open", return_value=stream), \
                patch("tools.experiment_session.subprocess.Popen", return_value=child), \
                patch("tools.experiment_session.os.killpg") as kill:
            with self.assertRaises(subprocess.TimeoutExpired):
                launch_benchmark(PROJECT, self.root / "trial", "synthetic-image", CONFIG, timeout_s=3)
        kill.assert_called_once_with(child.pid, signal.SIGTERM)
        self.assertEqual(child.wait.call_count, 2)


if __name__ == "__main__":
    unittest.main()
