# C20 bowl scientific reference

This directory preserves the NWChem QA input and output used to define the
larger-workload calibration. These are **upstream reference files, not CHIA
measurements**. Their original copyright/license notice is in `LICENSE.TXT`.

- Repository: <https://github.com/nwchemgit/nwchem>
- Tag: `v7.2.0-release`
- Revision: `d0d141fdfbf9595b9fd0ec3c7c803c6c5fe020b2`
- Source: `QA/chem_library_tests/C20/c20_bowl_ccpvdz_17_7_7.nw` and `.out`
- Exact source URLs and SHA256 values: [reference.json](reference.json)

The saved upstream output identifies **NWChem 6.8 and 128 processes**, even though
it is distributed in the pinned 7.2.0 source tree. It contains one RHF/cc-pVDZ
SCF result, **−756.702486554389 Hartree**, followed by TCE/CCSD calculations.
The SCF problem has 20 carbon atoms and 280 spherical basis functions. The
upstream full-job time includes coupled-cluster work and is not a timing
reference for our SCF-only case.

The derived [input](../../workload/inputs/c20_bowl_scf.nw) preserves the atomic
coordinates in bohr, C1 symmetry, spherical cc-pVDZ basis, singlet RHF method,
SCF thresholds (`thresh` and `tol2e` both `1e-10`), and memory settings. It changes
the title/start prefix, makes neutral charge explicit, removes the
machine-specific `permanent_dir`, and replaces the TCE task/settings with
`task scf energy`. Each launch gets a fresh case directory and no restart files.

The acceptance tolerance is fixed **before calibration** at `1e-6` Hartree.
Input bytes, normal completion, the expected SCF problem, and the energy are
checked. A failed check is retained as a failure; changing the reference or
tolerance requires a new documented protocol. No timing or scientific result
from the Perlmutter calibration is claimed by adding this input.

Memory directives are fixed workload settings inherited from the QA input.
Their presence is not memory measurement, tuning, or a memory-optimization claim.
