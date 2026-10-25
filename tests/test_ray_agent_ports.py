import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import check_ray_agent_ports as check


class AgentPortTests(unittest.TestCase):
    def test_explicit_ports_are_distinct_and_none_remain_auto_assigned(self):
        ports = check.select_ports()
        cmd = check.cli_command(Path("/tmp/example"), "127.0.0.1", ports)
        self.assertEqual(len(set(ports.values())), 5)
        for flag, port in ports.items():
            self.assertGreaterEqual(port, 1024)
            self.assertIn(flag + "=" + str(port), cmd)
        self.assertIn("--block", cmd)
        self.assertIn(str(check.PROJECT / "scripts/ray_port_forwarding.py"), cmd)
        invalid = {**ports, "--dashboard-agent-grpc-port": 0}
        with self.assertRaises(ValueError):
            check.cli_command(Path("/tmp/example"), "127.0.0.1", invalid)

    def test_dead_launcher_cannot_pass_using_a_readiness_message(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "ray-start.log"
            log.write_text("Ray runtime started.\n")
            proc = Mock()
            proc.poll.return_value = 1
            with self.assertRaises(RuntimeError):
                check.wait_for_cli(proc, log, timeout=0)

    def test_cleanup_escalates_only_the_group_created_for_this_launcher(self):
        proc = Mock(pid=12345)
        proc.wait.side_effect = [subprocess.TimeoutExpired("ray", 15), -9]
        with patch.object(check.os, "killpg") as kill:
            check.stop_owned_group(proc)
        self.assertEqual(kill.call_args_list, [
            unittest.mock.call(12345, signal.SIGTERM),
            unittest.mock.call(12345, signal.SIGKILL)])

    def test_raylet_fatal_error_stops_wait_without_extending_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "ray-start.log"
            log.write_text("starting\n")
            errors = root / "session_test/logs/raylet.err"
            errors.parent.mkdir(parents=True)
            errors.write_text("Check failed: missing port file\n")
            proc = Mock()
            proc.poll.return_value = None
            with self.assertRaisesRegex(RuntimeError, "Raylet fatal error"):
                check.wait_for_cli(proc, log, timeout=180, ray_tmp=root)

    def test_login_node_is_rejected_before_importing_ray(self):
        with patch.dict(os.environ, {"SLURM_JOB_ID": "123", "SLURM_CPUS_PER_TASK": "8"}), \
                patch.object(check.socket, "gethostname", return_value="login20"), \
                patch.object(check, "preflight") as preflight:
            with self.assertRaises(RuntimeError):
                check.run_probe()
            preflight.assert_not_called()

    def test_log_copy_skips_symlink_aliases_and_unrelated_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = root / "ray" / "session_test" / "logs"
            logs.mkdir(parents=True)
            (logs / "raylet.err").write_text("port timeout\n")
            (logs / "worker-private.txt").write_text("not part of this report")
            (root / "ray" / "session_latest").symlink_to(logs.parent, target_is_directory=True)
            result = check.copy_logs(root / "ray", root / "output")
            self.assertEqual(result, {"files": ["ray-logs/session_test/raylet.err"], "errors": []})


if __name__ == "__main__":
    unittest.main()
