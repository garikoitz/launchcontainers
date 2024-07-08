import os.path as op
import logging

logger = logging.getLogger("Launchcontainers")

def dwi_command(
    lc_config, sub, ses, ananlysis_dir
):
    """Puts together the command to send to the container.

    Args:
        lc_config (str): _description_
        sub (str): _description_
        ses (str): _description_
        ananlysis_dir (str): _description_

    Raises:
        ValueError: Raised in presence of a faulty config.yaml file, or when the formed command is not recognized.

    Returns:
        _type_: _description_
    """
    
    container = lc_config["general"]["container"]
    host = lc_config["general"]["host"]
    containerdir = lc_config["general"]["containerdir"]

    # Information relevant to the host and container
    jobqueue_config = lc_config["host_options"][host]
    version = lc_config["container_specific"][container]["version"]
    use_module = jobqueue_config["use_module"]
    bind_options = jobqueue_config["bind_options"]

    # Location of the Singularity Image File (.sif)
    container_name = op.join(containerdir, f"{container}_{version}.sif")
    
    # Define the directory and the file name to output the log of each subject
    logdir = op.join(ananlysis_dir, "sub-" + sub, "ses-" + ses, "output", "log")
    logfilename = f"{logdir}/t-{container}-sub-{sub}_ses-{ses}"

    subject_derivatives_path = op.join(ananlysis_dir, f"sub-{sub}", f"ses-{ses}")

    # Define the cmd goes before the main command
    bind_cmd = ""
    for bind in bind_options:
        bind_cmd += f"--bind {bind}:{bind} "

    env_cmd = ""
    if host == "local":
        if use_module == True:
            env_cmd = f"module load {jobqueue_config['apptainer']} && "
    
    # Define the main command
    if container in ["anatrois", "rtppreproc", "rtp-pipeline"]:
        logger.info("\n" + "start to generate the DWI PIPELINE command")
        logger.debug(
            f"\n the sub is {sub} \n the ses is {ses} \n the analysis dir is {ananlysis_dir}"
        )
        cmd = (
            f"{env_cmd}singularity run -e --no-home {bind_cmd}"
            f"--bind {subject_derivatives_path}/input:/flywheel/v0/input:ro "
            f"--bind {subject_derivatives_path}/output:/flywheel/v0/output "
            f"--bind {subject_derivatives_path}/output/log/config.json:/flywheel/v0/config.json "
            f"{container_name} 1>> {logfilename}.o 2>> {logfilename}.e "
        )

    if container == "freesurferator":
        logger.info("\n" + "FREESURFERATOR command")
        logger.debug(
            f"\n the sub is {sub} \n the ses is {ses} \n the analysis dir is {ananlysis_dir}"
        )
        cmd = (
            f"{env_cmd}apptainer run --containall --pwd /flywheel/v0 {bind_cmd}"
            f"--bind {subject_derivatives_path}/input:/flywheel/v0/input:ro "
            f"--bind {subject_derivatives_path}/output:/flywheel/v0/output "
            f"--bind {subject_derivatives_path}/work:/flywheel/v0/work "
            f"--bind {subject_derivatives_path}/output/log/config.json:/flywheel/v0/config.json "
            f"--env PATH=/opt/freesurfer/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/freesurfer/fsfast/bin:/opt/freesurfer/tktools:/opt/freesurfer/mni/bin:/sbin:/bin:/opt/ants/bin "
            f"--env LANG=C.UTF-8 "
            f"--env GPG_KEY=E3FF2839C048B25C084DEBE9B26995E310250568 "
            f"--env PYTHON_VERSION=3.9.15 "
            f"--env PYTHON_PIP_VERSION=22.0.4 "
            f"--env PYTHON_SETUPTOOLS_VERSION=58.1.0 "
            f"--env PYTHON_GET_PIP_URL=https://github.com/pypa/get-pip/raw/66030fa03382b4914d4c4d0896961a0bdeeeb274/public/get-pip.py "
            f"--env PYTHON_GET_PIP_SHA256=1e501cf004eac1b7eb1f97266d28f995ae835d30250bec7f8850562703067dc6 "
            f"--env FLYWHEEL=/flywheel/v0 "
            f"--env ANTSPATH=/opt/ants/bin/ "
            f"--env FREESURFER_HOME=/opt/freesurfer "
            f"--env FREESURFER=/opt/freesurfer "
            f"--env DISPLAY=:50.0 "
            f"--env FS_LICENSE=/flywheel/v0/work/license.txt "
            f"--env OS=Linux "
            f"--env FS_OVERRIDE=0 "
            f"--env FSF_OUTPUT_FORMAT=nii.gz "
            f"--env MNI_DIR=/opt/freesurfer/mni "
            f"--env LOCAL_DIR=/opt/freesurfer/local "
            f"--env FSFAST_HOME=/opt/freesurfer/fsfast "
            f"--env MINC_BIN_DIR=/opt/freesurfer/mni/bin "
            f"--env MINC_LIB_DIR=/opt/freesurfer/mni/lib "
            f"--env MNI_DATAPATH=/opt/freesurfer/mni/data "
            f"--env FMRI_ANALYSIS_DIR=/opt/freesurfer/fsfast "
            f"--env PERL5LIB=/opt/freesurfer/mni/lib/perl5/5.8.5 "
            f"--env MNI_PERL5LIB=/opt/freesurfer/mni/lib/perl5/5.8.5 "
            f"--env XAPPLRESDIR=/opt/freesurfer/MCRv97/X11/app-defaults "
            f"--env MCR_CACHE_ROOT=/flywheel/v0/output "
            f"--env MCR_CACHE_DIR=/flywheel/v0/output/.mcrCache9.7 "
            f"--env FSL_OUTPUT_FORMAT=nii.gz "
            f"--env ANTS_VERSION=v2.4.2 "
            f"--env QT_QPA_PLATFORM=xcb "
            f"--env PWD=/flywheel/v0 "
            f"{container_name} "
            f"-c python run.py 1> {logfilename}.o 2> {logfilename}.e "
        )

    if container == "rtp2-preproc":
        logger.info("\n" + "rtp2-preprc command")
        logger.debug(
            f"\n the sub is {sub} \n the ses is {ses} \n the analysis dir is {ananlysis_dir}"
        )

        cmd = (
            f"{env_cmd}apptainer run --containall --pwd /flywheel/v0 {bind_cmd}"
            f"--bind {subject_derivatives_path}/input:/flywheel/v0/input:ro "
            f"--bind {subject_derivatives_path}/output:/flywheel/v0/output "
            # f"--bind {subject_derivatives_path}/work:/flywheel/v0/work "
            f"--bind {subject_derivatives_path}/output/log/config.json:/flywheel/v0/config.json "
            f"--env FLYWHEEL=/flywheel/v0 "
            f"--env LD_LIBRARY_PATH=/opt/fsl/lib:  "
            f"--env FSLWISH=/opt/fsl/bin/fslwish  "
            f"--env FSLTCLSH=/opt/fsl/bin/fsltclsh  "
            f"--env FSLMULTIFILEQUIT=TRUE "
            f"--env FSLOUTPUTTYPE=NIFTI_GZ  "
            f"--env FSLDIR=/opt/fsl  "
            f"--env FREESURFER_HOME=/opt/freesurfer  "
            f"--env ARTHOME=/opt/art  "
            f"--env ANTSPATH=/opt/ants/bin  "
            f"--env PYTHON_GET_PIP_SHA256=1e501cf004eac1b7eb1f97266d28f995ae835d30250bec7f8850562703067dc6 "
            f"--env PYTHON_GET_PIP_URL=https://github.com/pypa/get-pip/raw/66030fa03382b4914d4c4d0896961a0bdeeeb274/public/get-pip.py "
            f"--env PYTHON_PIP_VERSION=22.0.4  "
            f"--env PYTHON_VERSION=3.9.15  "
            f"--env GPG_KEY=E3FF2839C048B25C084DEBE9B26995E310250568  "
            f"--env LANG=C.UTF-8  "
            f"--env PATH=/opt/mrtrix3/bin:/opt/ants/bin:/opt/art/bin:/opt/fsl/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin "
            f"--env PYTHON_SETUPTOOLS_VERSION=58.1.0 "
            f"--env DISPLAY=:50.0 "
            f"--env QT_QPA_PLATFORM=xcb  "
            f"--env FS_LICENSE=/opt/freesurfer/license.txt  "
            f"--env PWD=/flywheel/v0 "
            f"{container_name} "
            f"-c python run.py 1> {logfilename}.o 2> {logfilename}.e "
        )
    
    if container == "rtp2-pipeline":
        logger.info("\n" + "rtp2-pipeline command")
        logger.debug(
            f"\n the sub is {sub} \n the ses is {ses} \n the analysis dir is {ananlysis_dir}"
        )

        cmd = (
            f"{env_cmd}apptainer run --containall --pwd /flywheel/v0 {bind_cmd}"
            f"--bind {subject_derivatives_path}/input:/flywheel/v0/input:ro "
            f"--bind {subject_derivatives_path}/output:/flywheel/v0/output "
            # f"--bind {subject_derivatives_path}/work:/flywheel/v0/work "
            f"--bind {subject_derivatives_path}/output/log/config.json:/flywheel/v0/config.json "
            f"--env PATH=/opt/mrtrix3/bin:/opt/ants/bin:/opt/art/bin:/opt/fsl/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin "
            f"--env LANG=C.UTF-8 "
            f"--env GPG_KEY=E3FF2839C048B25C084DEBE9B26995E310250568 "
            f"--env PYTHON_VERSION=3.9.15 "
            f"--env PYTHON_PIP_VERSION=22.0.4 "
            f"--env PYTHON_SETUPTOOLS_VERSION=58.1.0 "
            f"--env PYTHON_GET_PIP_URL=https://github.com/pypa/get-pip/raw/66030fa03382b4914d4c4d0896961a0bdeeeb274/public/get-pip.py "
            f"--env PYTHON_GET_PIP_SHA256=1e501cf004eac1b7eb1f97266d28f995ae835d30250bec7f8850562703067dc6 "
            f"--env ANTSPATH=/opt/ants/bin "
            f"--env ARTHOME=/opt/art "
            f"--env FREESURFER_HOME=/opt/freesurfer "
            f"--env FSLDIR=/opt/fsl "
            f"--env FSLOUTPUTTYPE=NIFTI_GZ "
            f"--env FSLMULTIFILEQUIT=TRUE "
            f"--env FSLTCLSH=/opt/fsl/bin/fsltclsh "
            f"--env FSLWISH=/opt/fsl/bin/fslwish "
            f"--env LD_LIBRARY_PATH=/opt/mcr/v99/runtime/glnxa64:/opt/mcr/v99/bin/glnxa64:/opt/mcr/v99/sys/os/glnxa64:/opt/mcr/v99/extern/bin/glnxa64:/opt/fsl/lib: "
            f"--env FLYWHEEL=/flywheel/v0 "
            f"--env TEMPLATES=/templates "
            f"--env XAPPLRESDIR=/opt/mcr/v99/X11/app-defaults "
            f"--env MCR_CACHE_FOLDER_NAME=/flywheel/v0/output/.mcrCache9.9 "
            f"--env MCR_CACHE_ROOT=/flywheel/v0/output "
            f"--env MRTRIX_TMPFILE_DIR=/flywheel/v0/output/tmp "
            f"--env PWD=/flywheel/v0 "
            f"{container_name} "
            f"-c python run.py 1> {logfilename}.o 2> {logfilename}.e "
        )
    
    # If after all configuration, we do not have command, raise an error
    if cmd is None:
        logger.error(
            "\n"
            + f"the DWI PIPELINE command is not assigned, please check your config.yaml[general][host] session\n"
        )
        raise ValueError("cmd is not defined, aborting")

    
    return cmd
def py_command():
    #env_cmd = "conda init && conda activate votcloc &&"
    env_cmd= 'export PYTHONPATH=/bcbl/home/home_n-z/tlei/soft/MRIworkflow/Package/src:$PYTHONPATH '
  
    return env_cmd