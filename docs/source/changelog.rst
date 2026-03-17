Changelog
=========

0.4.7
-----

- **Logging**: replaced ad-hoc output redirection with a structured logging
  architecture. ``log_setup.py`` now provides ``_LoggingConsole``, a Rich
  ``Console`` subclass that auto-forwards every ``console.print()`` call to a
  Python ``logging.Logger``. Two log files are written per CLI command and
  copied into the analysis directory on completion:

  - ``<cmd>_<timestamp>.log`` — all messages (DEBUG and above)
  - ``<cmd>_<timestamp>.err`` — warnings and errors only

- **gen_jobscript package**: replaced the single ``gen_launch_cmd.py`` module
  with a ``gen_jobscript/`` package. The orchestrator (``__init__.py``) routes
  to per-type command builders:

  - ``gen_container_cmd.py`` — Apptainer/Singularity containers (existing logic)
  - ``gen_matlab_cmd.py`` — MATLAB script launcher (stub, raises ``NotImplementedError``)
  - ``gen_py_cmd.py`` — Python script launcher (stub, raises ``NotImplementedError``)

- **clusters/local.py**: renamed ``job_scheduler.py`` to ``local.py``. Added
  ``launch_serial()`` and ``launch_parallel()`` functions controlled by the new
  ``launch_mode`` yaml key. Each subprocess now runs inside ``bash -l`` so that
  ``module load`` commands work correctly. Parallel mode supports a
  ``mem_per_job`` memory ceiling enforced via ``resource.setrlimit`` on each
  worker process.

- **host_options.local** yaml keys updated: ``launch_mode`` (``serial`` /
  ``parallel``), ``max_workers`` (concurrent container limit), ``mem_per_job``
  (per-worker memory cap). Old Dask-era keys (``njobs``, ``memory_limit``,
  ``threads_per_worker``) are no longer used.

0.4.6
-----

- Added ``GLMPreparer`` and ``BasePreparer`` abstract class hierarchy for
  analysis-based (fMRI) pipelines.
- Introduced ``PREPARER_REGISTRY`` for zero-touch extension of new analysis types.
- ``cli.py``: two-path dispatch — legacy DWI path untouched; new registry path
  for analysis-based pipelines.
- Added ``glm_specific`` config section and ``glm_config_template.yaml``.

0.4.3
-----

- Improved temporal proximity matching for sbref↔bold pairing (180-second window).
- Fixed DWI file mapping for PA/AP direction naming.
- Fixed datetime parsing for ISO timestamp formats in scans.tsv validation.
- Fixed bidirectional file matching to always produce ``_dwi.nii.gz`` suffixes.

0.4.0
-----

- Initial public release of the ``lc`` CLI with ``prepare``, ``run``, ``qc``,
  ``copy_configs``, ``gen_subses``, and ``create_bids`` subcommands.
- SLURM (DIPC) and SGE (BCBL) scheduler support.
- Container-based pipeline support: ``anatrois``, ``freesurferator``,
  ``rtppreproc``, ``rtp-pipeline``, ``rtp2-preproc``, ``rtp2-pipeline``.
