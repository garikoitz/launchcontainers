Changelog
=========

0.4.7
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
