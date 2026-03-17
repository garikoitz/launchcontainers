Extending launchcontainers
==========================

Adding a new analysis type (e.g. ``prf``) requires only three steps and
touches no existing code.

Step 1 — Create a new Preparer class
--------------------------------------

Create ``launchcontainers/prepare/prf_preparer.py``:

.. code-block:: python

   from pathlib import Path
   from .base_preparer import BasePreparer, PrepIssue

   class PRFPreparer(BasePreparer):
       """Preparer for Population Receptive Field analysis."""

       ANALYSIS_TYPE = "prf"

       def check_requirements(self, sub: str, ses: str) -> list[PrepIssue]:
           """
           Return a list of PrepIssues for this subject/session.

           Severity levels:
             - "blocking"  → subject excluded from run
             - "warn"      → subject included, issue logged
             - "auto_fix"  → fix_fn() called automatically
           """
           issues = []
           bold_dir = self._bids_func_dir(sub, ses)
           if not bold_dir.exists():
               issues.append(PrepIssue(
                   sub=sub, ses=ses,
                   category="bold_dir",
                   severity="blocking",
                   message=f"Missing func dir: {bold_dir}",
               ))
           # add more checks here ...
           return issues

       def generate_run_script(self, sub: str, ses: str, analysis_dir: Path) -> Path:
           """Write and return the path to the HPC launch script."""
           host = self.host_options.get(self.general["host"], {})
           manager = host.get("manager", "local")
           if manager == "slurm":
               return self._slurm_script(sub, ses, analysis_dir)
           elif manager == "sge":
               return self._sge_script(sub, ses, analysis_dir)
           else:
               return self._local_script(sub, ses, analysis_dir)

       # --- private script builders ---

       def _slurm_script(self, sub, ses, analysis_dir) -> Path:
           ...

       def _sge_script(self, sub, ses, analysis_dir) -> Path:
           ...

       def _local_script(self, sub, ses, analysis_dir) -> Path:
           ...

The ``BasePreparer`` base class handles all orchestration — directory
creation, config freezing, issue processing, Rich summary output, and
log writing. Your subclass only needs to know what files the analysis
requires and how to write the launch script.

Step 2 — Register it
---------------------

Add one line to ``launchcontainers/prepare/__init__.py``:

.. code-block:: python

   from .base_preparer import BasePreparer, PrepIssue, PrepResult
   from .glm_preparer  import GLMPreparer
   from .prf_preparer  import PRFPreparer          # ← add this

   PREPARER_REGISTRY: dict[str, type[BasePreparer]] = {
       "glm": GLMPreparer,
       "prf": PRFPreparer,                         # ← and this
   }

Step 3 — Set the config
------------------------

In ``lc_config.yaml``, set ``general.container`` to the new key:

.. code-block:: yaml

   general:
     container: prf

Then run ``lc prepare`` as normal — the registry dispatches automatically.

BasePreparer API
----------------

The following attributes and methods are available to all subclasses.

**Config accessors** (read from ``lc_config.yaml``):

.. list-table::
   :header-rows: 1
   :widths: 30 55

   * - Attribute
     - Description
   * - ``self.general``
     - ``dict`` — the ``general`` section of the config
   * - ``self.analysis_specific``
     - ``dict`` — the ``<type>_specific`` section (e.g. ``glm_specific``)
   * - ``self.host_options``
     - ``dict`` — the ``host_options`` section

**Utility methods**:

.. list-table::
   :header-rows: 1
   :widths: 40 45

   * - Method
     - Description
   * - ``self.copy_file_to_analysis(src, dest_name, force)``
     - Copy a file into the analysis ``config/`` directory
   * - ``self._get_analysis_dir()``
     - Returns the ``Path`` to the analysis root directory
   * - ``self._create_dir_structure()``
     - Creates ``config/``, ``logs/``, ``scripts/``, ``results/``

Adding a new integrity checker spec
-------------------------------------

To add a matching QC spec for the ``checker`` tool, subclass
``AnalysisSpec`` in ``analysis_checker/``:

.. code-block:: python

   from analysis_checker.base_spec import AnalysisSpec

   class PRFSpec(AnalysisSpec):
       SPEC_NAME = "prf"

       def expected_files(self, sub, ses) -> list[str]:
           ...

Then register it in the checker's ``SPEC_REGISTRY``. The same
``checker --spec prf`` command will then validate PRF outputs.
