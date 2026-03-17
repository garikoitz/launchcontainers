Code Structure
==============

Repository layout
-----------------

.. code-block:: text

   launchcontainers/
   ├── cli.py                       ← entry point; argparse subcommands
   ├── do_prepare.py                ← prepare orchestration (DWI legacy path)
   ├── do_launch.py                 ← job submission
   ├── do_qc.py                     ← quality control
   ├── utils.py                     ← shared utilities (read_yaml, read_df, …)
   ├── config_logger.py             ← logging setup
   │
   ├── prepare/
   │   ├── __init__.py              ← PREPARER_REGISTRY
   │   ├── base_preparer.py         ← BasePreparer, PrepIssue, PrepResult
   │   ├── glm_preparer.py          ← GLMPreparer
   │   ├── prepare_dwi.py           ← DWI container preparation (legacy)
   │   └── dwi_prepare_input.py
   │
   ├── clusters/
   │   ├── slurm.py                 ← SLURM job submission helpers
   │   ├── sge.py                   ← SGE job submission helpers
   │   └── dask_scheduler.py        ← local parallel execution via Dask
   │
   ├── check/
   │   ├── general_checks.py        ← shared file-existence checks
   │   └── check_dwi_pipelines.py   ← DWI-specific checks
   │
   └── helper_function/
       ├── gen_subses.py            ← subseslist generation
       ├── create_bids.py           ← fake BIDS structure for testing
       ├── copy_configs.py          ← copy example configs to working dir
       └── zip_example_config.py    ← archive configs (developer utility)

Entry points (``pyproject.toml``)
-----------------------------------

.. code-block:: toml

   [tool.poetry.scripts]
   lc      = "launchcontainers.cli:main"
   checker = "analysis_checker.check_analysis_integrity:main"

Dispatch logic in ``cli.py``
------------------------------

``lc prepare`` uses a two-path dispatch based on the ``container`` key:

.. code-block:: python

   if container in PREPARER_REGISTRY:
       # New-style: BasePreparer subclass owns all preparation logic
       cls = PREPARER_REGISTRY[container]
       preparer = cls(config=lc_config, subseslist=subsesrows, output_root=output_root)
       preparer.run(dry_run=False)
   else:
       # Legacy DWI path (untouched)
       do_prepare.main(parse_namespace, analysis_dir)

This means all DWI container pipelines use the original ``do_prepare`` code,
while analysis-based pipelines (``glm``, ``prf``, …) go through the
``BasePreparer`` class hierarchy. Adding a new analysis type never requires
modifying the legacy path.

BasePreparer class hierarchy
-----------------------------

.. code-block:: text

   BasePreparer  (abstract)
   ├── GLMPreparer
   └── PRFPreparer  (planned)

``BasePreparer`` owns all orchestration: directory creation, config
freezing, issue processing, Rich summary printing, and log writing.
Subclasses implement only two abstract methods:

- ``check_requirements(sub, ses) → list[PrepIssue]``
- ``generate_run_script(sub, ses, analysis_dir) → Path``

Data classes
------------

.. code-block:: python

   @dataclass
   class PrepIssue:
       sub:      str
       ses:      str
       category: str
       severity: str        # "blocking" | "warn" | "auto_fix"
       message:  str
       fix_fn:   Callable | None = None

   @dataclass
   class PrepResult:
       sub:    str
       ses:    str
       status: str          # "ready" | "fixed" | "warn" | "blocked"
       issues: list[PrepIssue]
