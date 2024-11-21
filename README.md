![logo launchcontainers](https://user-images.githubusercontent.com/48440236/262432254-c7b53943-7c90-489c-933c-5f5a32510db4.png)
# launchcontainers
**Launchcontainers** is a Python-based tool for automatically launching computing works on HPC or local nodes. It helps you: 
1. Prepare folder structures and input files automatically
2. Backup the input configs for data provenance
3. Launching containers in local HPC, SGE or SLURM in serial and in parallel

Currently, **launchcontainers** works along with [anatROIs](https://github.com/garikoitz/anatROIs), [RTP-preproc](https://github.com/garikoitz/rtp-preproc), and [RTP2-pipeline](https://github.com/garikoitz/rtp-pipeline).

To use the newest version, please `pip install launchcontainers==0.3.50 `


## NEW FEATURES
* The update `0.3.5` will be capable work with heudiconv, Presurfer and NORDIC_raw, there will be a new derivatives folder called Processed_nifti, it will stored the processed .nii.gz by NORDIC_raw and Presurfer
* `0.3.5`: The add_intended_for function from heudiconv will be used here to edit the fmap _epi.json
* Add requests into pyproject.toml, remove version limit to common package such as nibabel and numpy 
* Changed rtp/rtp2-preproc multishell option to separateed_shell_files
* Edited lc_config.yaml comment about dask_worker options
* Fixed error message by dask progress (0.3.18)
* launchcontainers --copy_configs "~/path/to/working_directory" will copy the corresponding config files to your specified directory!
* We updated the lc_config.yaml for RTP2-pipelines, please have a look!

check the [How to use]() for more information

# Check also:
* [Home](https://github.com/garikoitz/launchcontainers/wiki/Home)
* [Installation](https://github.com/garikoitz/launchcontainers/wiki/Installation)
* [Manual](https://github.com/garikoitz/launchcontainers/wiki/Manual)
    - [Edit configs](https://github.com/garikoitz/launchcontainers/wiki/Manual)
    - [Launch `prepare` mode](https://github.com/garikoitz/launchcontainers/wiki/Manual)
    - [Launch `run` mode](https://github.com/garikoitz/launchcontainers/wiki/Manual)
* [Reporting, Contribution, and citation](https://github.com/garikoitz/launchcontainers/wiki/Reporting,-Contributing,-and-Citation)
* [Supplement: How to use HeuDiConv](https://github.com/garikoitz/launchcontainers/wiki/How-to-Use-HeuDiConv)
