"""Native launch tests use mocked processes, never synthetic research results."""
import tempfile
import unittest
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch
from workload.nwchem_benchmark import run_case, native_cpu_slots
from tools.experiment_session import ExperimentSession, configuration

class NativeTests(unittest.TestCase):
    def test_native_launch_uses_cpu_set_and_returns_slot(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / 'water.nw'
            source.write_text('test')
            slots = Queue()
            slots.put([2, 4])
            args = SimpleNamespace(input=source, mpi_tasks=2, cpus_per_task=1,
                                   image='unused', backend='native',
                                   nwchem='/usr/bin/nwchem.openmpi', cpu_slots=slots)
            def fake_process(command, **kwargs):
                kwargs['stdout'].write(b'Total SCF energy = -75.9839975704\n')
                return Mock(returncode=0)
            with patch('workload.nwchem_benchmark.subprocess.run', side_effect=fake_process) as run:
                result = run_case('p0-r0', 0, 0, args, root)
            self.assertTrue(result['passed'])
            self.assertEqual(run.call_args.args[0], ['taskset', '--cpu-list', '2,4',
                'mpirun', '--bind-to', 'none', '-np', '2', '/usr/bin/nwchem.openmpi', 'water.nw'])
            self.assertEqual(run.call_args.kwargs['env']['OMP_NUM_THREADS'], '1')
            self.assertEqual(slots.get_nowait(), [2, 4])
            slots.put([2, 4])
            with patch('workload.nwchem_benchmark.subprocess.run', side_effect=OSError('mock')):
                with self.assertRaises(OSError):
                    run_case('p1-r0', 1, 0, args, root)
            self.assertEqual(slots.get_nowait(), [2, 4])

    def test_smt_siblings_not_allocated_as_separate_cores(self):
        def topology(path, *args, **kwargs):
            cpu = int(str(path).split('/cpu/cpu')[1].split('/')[0])
            return '0' if path.name == 'physical_package_id' else str(cpu // 2)
        with patch('os.sched_getaffinity', return_value=set(range(16))), \
             patch.object(Path, 'read_text', topology):
            slots = native_cpu_slots(3, 2)
            assigned = [slots.get_nowait() for _ in range(3)]
            self.assertEqual(assigned, [[0, 2], [4, 6], [8, 10]])
            with self.assertRaises(ValueError):
                native_cpu_slots(4, 2)

    def test_native_session_passes_backend_to_worker_runner(self):
        with tempfile.TemporaryDirectory() as folder:
            baseline = {'configuration': configuration(1, 1), 'receipt': 'base',
                        'step': 0, 'status': 'success', 'selection_source': 'scripted_baseline',
                        'aggregate': {'throughput_cases_per_s': 1.0}}
            session = ExperimentSession.create(folder, folder, 'unused', 4, baseline, [], backend='native')
            history = session.history()
            self.assertEqual(len(history['untested_configurations']), 8)
            with patch('tools.experiment_session.run_benchmark', return_value={
                    'aggregate': {'throughput_cases_per_s': 1.2}}) as runner:
                result = session.experiment(2, 2, 'test concurrency', 'base')
            self.assertEqual(result['status'], 'success')
            self.assertEqual(runner.call_args.kwargs['backend'], 'native')

if __name__ == '__main__':
    unittest.main()
