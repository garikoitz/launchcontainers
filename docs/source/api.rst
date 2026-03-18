.. _api_ref:

API Reference
=============

This page documents the main public modules in ``launchcontainers`` using the
package docstrings and function docstrings pulled directly from the source.

Core CLI
--------

.. automodule:: launchcontainers.cli
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.do_prepare
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.do_launch
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.do_qc
   :members:
   :undoc-members: False

Utilities
---------

.. automodule:: launchcontainers.utils
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.log_setup
   :members:
   :undoc-members: False

Job-script generation
---------------------

.. automodule:: launchcontainers.gen_jobscript
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.gen_jobscript.gen_container_cmd
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.gen_jobscript.gen_matlab_cmd
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.gen_jobscript.gen_py_cmd
   :members:
   :undoc-members: False

Preparation
-----------

.. automodule:: launchcontainers.prepare.dwi_prepare
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.prepare.RTP2_prepare_input
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.prepare.gen_bids_derivatives
   :members:
   :undoc-members: False

fMRI-GLM preparation
~~~~~~~~~~~~~~~~~~~~

Overview
^^^^^^^^

The sole purpose of the GLM preparation stage is to **match behavioural log
files to their corresponding MRI acquisitions** and then **organise the
resulting file tree so that the GLM container can consume it without any
further renaming or path resolution**.

Concretely, the pipeline does two things:

1. **Parse the vistadisplog** — for every ``20*.mat`` log file found under
   ``<bidsdir>/sourcedata/vistadisplog/sub-<sub>/ses-<ses>/``, read the
   embedded stimulus filename (``params.loadMatrix``) to determine the task
   and condition.  Derive ``onset``, ``duration``, and ``trial_type`` columns
   and write one BIDS-compliant ``events.tsv`` per run.  A companion mapping
   TSV (``sub-<sub>_ses-<ses>_desc-mapping_PRF_acqtime.tsv``) is written to
   the same vistadisplog directory and records the acquisition time of each
   log, which is used in the next step.

2. **Match logs → NIfTIs and create symlinks** — each raw BIDS bold file
   carries an ``AcquisitionTime`` field in its JSON sidecar.  The pipeline
   compares this time against the mapping TSV (within a ±120 s window) to
   identify which log corresponds to which bold file.  Once matched, it
   creates symlinks for both the NIfTI and its JSON sidecar, renaming them
   with a normalised GLM task-run label (e.g. ``task-fixnonstop_run-01``).
   The same renaming is applied to the corresponding fMRIprep preprocessed
   bold (and its JSON sidecar).

No data are copied.  Everything downstream sees a coherent, consistently
named file tree that is entirely made of symlinks pointing back into the
original BIDS and fMRIprep directories.

Word-Center (WC) mode
^^^^^^^^^^^^^^^^^^^^^

When ``container_specific.fMRI-GLM.is_WC`` is ``True`` the pipeline runs in
*Word-Center* mode.  In this mode the output is written to a separate
directory tree rooted at ``<basedir>/<output_bids>`` (e.g. ``BIDS_WC``)
instead of being placed inside the original BIDS tree.  The layout mirrors
the standard BIDS structure:

.. code-block:: text

   <basedir>/
     <output_bids>/                          # e.g. BIDS_WC/
       sub-<sub>/ses-<ses>/func/
         *_task-fixnonstop_run-01_bold.nii.gz   → symlink → original BIDS
         *_task-fixnonstop_run-01_bold.json      → symlink → original BIDS JSON
         *_task-fixnonstop_run-01_events.tsv     (written directly)
       derivatives/
         <fmriprep_analysis_name>/
           sub-<sub>/ses-<ses>/func/
             *_task-fixnonstop_run-01_..._bold.func.gii  → symlink → fMRIprep
             *_task-fixnonstop_run-01_..._bold.json       → symlink → fMRIprep JSON

Configuration
^^^^^^^^^^^^^

The pipeline is configured entirely through the
``container_specific.fMRI-GLM`` section of the launchcontainers YAML.  If
that section is absent, calling
:func:`~launchcontainers.prepare.glm_prepare.run_glm_prepare` (or the
``lc prepare`` CLI command) will write an annotated example config to
``lc_config_example.yaml`` in the current directory.

API
^^^

.. automodule:: launchcontainers.prepare.glm_prepare
   :members:
   :undoc-members: False

.. autoclass:: launchcontainers.prepare.glm_prepare.GLMPrepare
   :members:
   :private-members: _load_mapping_tsv
   :show-inheritance:

.. automodule:: launchcontainers.prepare.prf_prepare
   :members:
   :undoc-members: False

Schedulers
----------

.. automodule:: launchcontainers.clusters.local
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.clusters.slurm
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.clusters.sge
   :members:
   :undoc-members: False

Checks And QC
-------------

.. automodule:: launchcontainers.check.check_dwi_pipelines
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.check.general_checks
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.quality_control.continue_run_rtp2pipeline
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.quality_control.qc_rtp2preproc_output
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.quality_control.qc_tract_finish_rtp2pipeline
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.quality_control.rtp2pipelne_unzip_output
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.quality_control.batch_sync_tract_and_mrtrix
   :members:
   :undoc-members: False


Helper Scripts
--------------

.. automodule:: launchcontainers.helper_function.copy_configs
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.helper_function.create_bids
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.helper_function.gen_subses
   :members:
   :undoc-members: False

.. automodule:: launchcontainers.helper_function.zip_example_config
   :members:
   :undoc-members: False
