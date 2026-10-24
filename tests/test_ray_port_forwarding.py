import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from contextlib import nullcontext
from unittest.mock import Mock, patch

from scripts import ray_port_forwarding as fix


class ForwardingTests(unittest.TestCase):
    def setUp(self):
        self.ports = dict(zip(fix.PORT_MAP.values(), (24001, 24002, 24003, 24004)))
        self.command = [
            "/raylet", "--metrics-agent-port=24001", "--metrics_export_port=24002",
            "--runtime_env_agent_port=24004",
            "--dashboard_agent_command=" + subprocess.list2cmdline([
                "/python", "/agent.py", "--grpc-port=24001", "--metrics-export-port=24002",
                "--listen-port=24003", "--logging-format=message with spaces"]),
            "--runtime_env_agent_command=/python /runtime.py --runtime-env-agent-port=24004",
        ]

    def test_missing_port_added_to_raylet_and_input_preserved(self):
        before = list(self.command)
        command, report = fix.repair_raylet_command(self.command, self.ports)
        self.assertEqual(self.command, before)
        self.assertEqual(command, before + ["--dashboard_agent_listen_port=24003"])
        self.assertTrue(report["added_missing_dashboard_port"])
        self.assertEqual(report["agent_ports"], report["raylet_ports"])

    def test_already_forwarded_port_is_not_duplicated(self):
        self.command.append("--dashboard-agent-listen-port=24003")
        command, report = fix.repair_raylet_command(self.command, self.ports)
        self.assertEqual(command, self.command)
        self.assertFalse(report["added_missing_dashboard_port"])

    def test_conflicting_and_duplicate_ports_rejected(self):
        for additions in (["--dashboard_agent_listen_port=24005"],
                          ["--dashboard_agent_listen_port=0"],
                          ["--dashboard_agent_listen_port=24003", "--dashboard-agent-listen-port=24003"],
                          ["--metrics-agent-port=24001"]):
            with self.subTest(additions=additions), self.assertRaises(ValueError):
                fix.repair_raylet_command(self.command + additions, self.ports)

    def test_nested_agent_mismatch_rejected_before_dispatch(self):
        command = [part.replace("--listen-port=24003", "--listen-port=24005") for part in self.command]
        with tempfile.TemporaryDirectory() as directory:
            original = Mock()
            services = SimpleNamespace(start_ray_process=original)
            with fix.forwarded_launches(services, self.ports, Path(directory) / "record.json"):
                with self.assertRaises(ValueError):
                    services.start_ray_process(command, "raylet", fate_share=True)
            original.assert_not_called()

    def test_launch_context_records_ports_preserves_arguments_and_restores(self):
        with tempfile.TemporaryDirectory() as directory:
            record = Path(directory) / "record.json"
            original = Mock(return_value="process")
            services = SimpleNamespace(start_ray_process=original, __file__=__file__)
            with fix.forwarded_launches(services, self.ports, record):
                self.assertEqual(services.start_ray_process(self.command, "raylet", True, cwd="/tmp"), "process")
            original.assert_called_once_with(
                self.command + ["--dashboard_agent_listen_port=24003"], "raylet", True, cwd="/tmp")
            self.assertIs(services.start_ray_process, original)
            data = json.loads(record.read_text())
            self.assertEqual(data["raylet_ports"], self.ports)
            self.assertEqual(len(data["ray_services_sha256"]), 64)

    def test_unrelated_process_passthrough_and_exception_restores(self):
        original = Mock(side_effect=RuntimeError("dispatch failure"))
        services = SimpleNamespace(start_ray_process=original)
        command = ["/gcs", "--some-argument=1"]
        with self.assertRaises(RuntimeError):
            with fix.forwarded_launches(services, self.ports, Path("unused-record.json")):
                services.start_ray_process(command, "gcs_server", False)
        original.assert_called_once_with(command, "gcs_server", False)
        self.assertIs(services.start_ray_process, original)

    def test_cli_port_validation(self):
        args = [key + "=" + str(self.ports[value]) for key, value in fix.PORT_MAP.items()]
        self.assertEqual(fix.expected_ports(args), self.ports)
        for invalid in (args[:-1], args + [args[0]], [value.replace("24004", "0") for value in args]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                fix.expected_ports(invalid)

    def test_helper_login_guard_precedes_ray_import(self):
        with patch.dict(os.environ, {"SLURM_JOB_ID": "123", "SLURM_CPUS_PER_TASK": "8"}), \
                patch.object(fix.socket, "gethostname", return_value="login35"), \
                patch.object(fix.importlib, "import_module") as load:
            with self.assertRaises(RuntimeError):
                fix.run_cli(["start", "--head", "--block"], Path("unused.json"))
            load.assert_not_called()

    def test_c20_allocation_is_accepted_and_smaller_allocation_rejected(self):
        cli = Mock()
        modules = {"ray": SimpleNamespace(__version__="2.54.0"),
                   "ray._private.services": SimpleNamespace(),
                   "ray.scripts.scripts": SimpleNamespace(cli=cli)}
        args = ["start", "--head", "--block"] + [
            key + "=" + str(self.ports[value]) for key, value in fix.PORT_MAP.items()]
        with patch.dict(os.environ, {"SLURM_JOB_ID": "123", "SLURM_NTASKS": "8", "SLURM_CPUS_PER_TASK": "2"}), \
                patch.object(fix.socket, "gethostname", return_value="nid-test"), \
                patch.object(fix.importlib, "import_module", side_effect=modules.__getitem__), \
                patch.object(fix, "forwarded_launches", return_value=nullcontext()):
            fix.run_cli(args, Path("unused.json"))
            cli.assert_called_once_with(args=args, prog_name="ray")
            with patch.dict(os.environ, {"SLURM_NTASKS": "1"}):
                with self.assertRaises(RuntimeError):
                    fix.run_cli(args, Path("unused.json"))


if __name__ == "__main__":
    unittest.main()
