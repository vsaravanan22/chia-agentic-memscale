# Ray 2.54.0 startup diagnostic: HTTP-port forwarding

This is a diagnostic-only, process-local compatibility correction. It does
not change the installed Ray package, the scientific runner, or a frozen C20
plan. A successful startup diagnostic is not a successful C20 calibration.

## Observed failure

Job `60149097` on `nid004379` failed before any scientific workload launched.
Its command supplied `--dashboard-agent-listen-port=46317`. Nevertheless,
`raylet.err` reported a fatal timeout at `node_manager.cc:3321` waiting for
`dashboard_agent_listen_port_<node-id>`. The outer diagnostic then expired
while waiting for the CLI's readiness message.

Ray 2.54.0's `start_raylet()` passes the chosen HTTP port to the nested
dashboard-agent command as `--listen-port`, but omits the corresponding
`--dashboard_agent_listen_port` flag from the raylet command itself. The C++
default is zero, which selects the port-file wait. Supplying the public CLI
option alone therefore does not bypass that wait. The earlier CLI-options
preflight missed this forwarding omission.

Primary sources, pinned to the installed release:

- [Python command builder](https://github.com/ray-project/ray/blob/ray-2.54.0/python/ray/_private/services.py)
- [Raylet flag defaults](https://github.com/ray-project/ray/blob/ray-2.54.0/src/ray/raylet/main.cc)
- [Port-file wait](https://github.com/ray-project/ray/blob/ray-2.54.0/src/ray/raylet/node_manager.cc)

The reason dashboard agents are slow to initialize on these allocations has
not been established. This correction addresses the demonstrated missing
argument; it does not establish that every startup problem is resolved.

## Correction and checks

`scripts/ray_port_forwarding.py` wraps the Ray CLI inside its own process.
Immediately before Ray dispatches the raylet process, it:

1. Requires Ray 2.54.0 and four distinct, explicit agent ports.
2. Checks both nested agent commands against the requested ports.
3. Adds the dashboard HTTP-port argument only if absent from the raylet
   command. Existing correct arguments are retained; conflicting or duplicate
   arguments fail before raylet dispatch.
4. Verifies all four forwarded raylet ports and records the installed services
   source SHA256 and port mapping in `port-forwarding.json`.

Other process launches pass through unchanged. The wrapper restores the
original function when the CLI exits. It writes no port readiness files and
changes no site-packages files. Source snapshots include the wrapper.

The no-launch preflight invokes the installed `start_raylet()` command builder
with process dispatch intercepted and `subprocess.Popen` forbidden. This
checks the actual installed forwarding path, rather than only CLI option
availability. The local development check used Ray 2.54.0 on Python 3.12;
run the same preflight in the Perlmutter Python 3.10 environment before submission.

The live diagnostic retains its original time limits and CPU allocation.
It requires CLI readiness, all four listening agent ports, and a response from
a separate Ray worker on the same node. Fatal raylet checks now end the wait
early. `port-forwarding.json` is evidence of arguments, not successful startup.

## Run from the repository root

```bash
python -m unittest discover -s tests -p 'test_ray_*.py' -v
python scripts/check_ray_agent_ports.py --preflight
bash -n scripts/check_ray_agent_ports.sbatch
sbatch scripts/check_ray_agent_ports.sbatch
```

Only submit after the preceding checks pass. This launches no NWChem or model
requests. Results use a fresh `results/ray-agent-ports-JOBID-TIMESTAMP/` folder.
Preserve the failed job's evidence. If the diagnostic passes, integrating this
startup path into C20 requires a deliberate runner change, tests, and a newly
committed frozen plan. Do not reuse an old plan after changing its sources.
