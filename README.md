![logo launchcontainers](https://user-images.githubusercontent.com/48440236/262432254-c7b53943-7c90-489c-933c-5f5a32510db4.png)
# launchcontainers
**Launchcontainers** is a Python-based tool for automatically launching parallel computing tasks on HPC or local clusters. It was designed to: 
1. Prepare folder structures and input files automatically for Neuroimaging pipelines
2. Backup the input configs for data provenance
3. Deploy jobs in local HPC, SGE, or SLURM in parallel

Currently, **launchcontainers** works along with [anatROIs](https://github.com/garikoitz/anatROIs), [RTP-preproc](https://github.com/garikoitz/rtp-preproc), and [RTP2-pipeline](https://github.com/garikoitz/rtp-pipeline).

To use the newest version, please `pip install launchcontainers==0.4.2 `


## NEW FEATURES
* Update to `0.4.2`. Refactor the launchcontainer command-line interaction to make it more user-friendly
    * For *prepare mode*, the user will do: `lc --log-dir path/to/log/dir prepare -lcc path/to/lc_yaml -ssl path/to/subseslist -cc path/to/cc`
        * The prepare mode will create symlink and prepare analysis folder structure. it will output analysis_dir in the commandline for *run mode*
    * For *run mode*, the user will do: `lc --log-dir path/to/log/dir run -w path/to/analysis_dir --run_lc`
        * The run mode will do a independent check on analysis dir to see if the configs there is correct
        * Then it will summarize the config settings and folder structure (using the cli tree command) to the user and ask for user input
        * Once user type y or yes, the program will launch
    * Several helper functions also implemented in the *cli* `lc `:
        * `lc --copy_configs -o path/to/working/dir`  you can type this to copy all the example configs to the working directory
        * `lc --create_bids -csc path/to/csc/yaml -ssl path/to/subseslist `  you can use this to create a fake BIDS folder for testing
    * for more info, pip install lc and type `lc -h`
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
