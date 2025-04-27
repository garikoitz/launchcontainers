"""
MIT License

Copyright (c) 2020-2023 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2024 Leandro Lecca
Copyright (c) 2022-2023 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge,
to any person obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.
"""
from __future__ import annotations

import json
import logging
import os
import os.path as op

from launchcontainers import utils as do
from launchcontainers.prepare import dwi_prepare_input as prep_dwi
logger = logging.getLogger('Launchcontainers')


def copy_configs(container, extra_config_fpath, analysis_dir, force, option=None):
    if os.path.isfile(extra_config_fpath):
        logger.info(
            '\n'
            + f' We will pass  {extra_config_fpath} to {container} analysis dir',
        )
    else:
        logger.error(
            '\n'
            f'{extra_config_fpath} does not exist',
        )

    config_fname = os.path.basename(extra_config_fpath)
    file_suffix = os.path.splitext(config_fname)[1]

    src_fpath = extra_config_fpath
    dst_fpath = os.path.join(analysis_dir, config_fname)
    if container in ['anatrois', 'freesurferator']:
        if file_suffix in ['.nii', '.gz', '.zip']:
            do.copy_file(src_fpath, dst_fpath, force)
        else:
            raise ValueError('Unsupported file type.')
    if container in ['rtp2-preproc', 'rtppreproc']:
        # there are no extra configs for rtp-preproc or rtp2-preproc
        pass

    if container in ['rtp2-pipeline', 'rtp-pipeline']:
        if option == 'tractparams':
            if file_suffix in ['.csv']:
                do.copy_file(src_fpath, dst_fpath, force)
            else:
                raise ValueError('Unsupported file type.')
        if option == 'fsmask':
            if file_suffix in ['.nii', '.gz']:
                do.copy_file(src_fpath, dst_fpath, force)
            else:
                raise ValueError('Unsupported file type.')
    return config_fname


def gen_config_dict_and_copy(parser_namespace):
    '''
    This function is used to copy other config files to the analysis folder

    and will have a dictionary that stores all the info
    '''
    # read the yaml to get input info
    lc_config_fpath = parser_namespace.lc_config
    lc_config = lc_config = do.read_yaml(lc_config_fpath)
    logger.info('\n prepare_dwi_extra_configs reading lc config yaml')
    # read parameters from lc_config
    basedir = lc_config['general']['basedir']
    container = lc_config['general']['container']
    force = lc_config['general']['force']
    analysis_name = lc_config['general']['analysis_name']
    version = lc_config['container_specific'][container]['version']
    bidsdir_name = lc_config['general']['bidsdir_name']
    container_folder = op.join(
        basedir,
        bidsdir_name,
        'derivatives',
        f'{container}_{version}',
    )
    analysis_dir = op.join(container_folder, f'analysis-{analysis_name}')
    container_configs_fname = f'{container}.json'
    json_under_analysis_dir = op.join(analysis_dir, container_configs_fname)
    # set up dict to get extra config infomation
    config_json_dict = {}
    config_json_dict['config_path'] = json_under_analysis_dir

    # store the json content into dict and copy the extract configs to analysis folder
    if container in ['anatrois']:
        config_json_dict[container] = {}
        pre_fs = lc_config['container_specific'][container]['pre_fs']
        annotfile = lc_config['container_specific'][container]['annotfile']
        mniroizip = lc_config['container_specific'][container]['mniroizip']
        if pre_fs:
            config_fname = 'existingFS.zip'
            config_json_dict[container]['pre_fs'] = f'pre_fs/{config_fname}'
        else:
            config_fname = 'T1.nii.gz'
            config_json_dict[container]['anat'] = f'anat/{config_fname}'
        if annotfile:
            config_fname = copy_configs(container, annotfile, analysis_dir, force)

            config_json_dict[container]['annotfile'] = f'annotfile/{config_fname}'
        if mniroizip:
            config_fname = copy_configs(container, mniroizip, analysis_dir, force)
            config_json_dict[container]['mniroizip'] = f'mniroizip/{config_fname}'

    if container in ['freesurferator']:
        config_json_dict[container] = {}
        pre_fs = lc_config['container_specific'][container]['pre_fs']
        control_points = lc_config['container_specific'][container]['control_points']
        annotfile = lc_config['container_specific'][container]['annotfile']
        mniroizip = lc_config['container_specific'][container]['mniroizip']
        if pre_fs:
            config_fname = 'existingFS.zip'
            config_json_dict[container]['pre_fs'] = f'pre_fs/{config_fname}'
        else:
            config_fname = 'T1.nii.gz'
            config_json_dict[container]['anat'] = f'anat/{config_fname}'
        if control_points:
            config_fname = 'control.dat'
            config_json_dict[container]['control_points'] = f'control_points/{config_fname}'
        if annotfile:
            config_fname = copy_configs(container, annotfile, analysis_dir, force)
            config_json_dict[container]['annotfile'] = f'annotfile/{config_fname}'
        if mniroizip:
            config_fname = copy_configs(container, mniroizip, analysis_dir, force)
            config_json_dict[container]['mniroizip'] = f'mniroizip/{config_fname}'

    if container in ['rtppreproc']:
        preproc_json_keys = ['ANAT', 'BVAL', 'BVEC', 'DIFF', 'FSMASK']
        preproc_json_val = [
            'ANAT/T1.nii.gz', 'BVAL/dwiF.bval',
            'BVEC/dwiF.bvec', 'DIFF/dwiF.nii.gz', 'FSMASK/brain.nii.gz',
        ]
        config_json_dict[container] = {
            key: value for key,
            value in zip(preproc_json_keys, preproc_json_val)
        }
        rpe = lc_config['container_specific'][container]['rpe']
        if rpe:
            config_json_dict[container]['RBVC'] = 'RBVC/dwiR.bvec'
            config_json_dict[container]['RBVL'] = 'RBVL/dwiR.bval'
            config_json_dict[container]['RDIF'] = 'RDIF/dwiR.nii.gz'

    if container in ['rtp2-preproc']:
        preproc_json_keys = ['ANAT', 'BVAL', 'BVEC', 'DIFF', 'FSMASK']
        preproc_json_val = [
            'ANAT/T1.nii.gz', 'BVAL/dwiF.bval',
            'BVEC/dwiF.bvec', 'DIFF/dwiF.nii.gz', 'FSMASK/brain.nii.gz',
        ]
        config_json_dict[container] = {
            key: value for key,
            value in zip(preproc_json_keys, preproc_json_val)
        }

        rpe = lc_config['container_specific'][container]['rpe']
        use_qmap = lc_config['container_specific'][container]['use_qmap']
        if rpe:
            config_json_dict[container]['RBVC'] = 'RBVC/dwiR.bvec'
            config_json_dict[container]['RBVL'] = 'RBVL/dwiR.bval'
            config_json_dict[container]['RDIF'] = 'RDIF/dwiR.nii.gz'
        if use_qmap:
            config_fname = 'qmap.zip'
            config_json_dict[container]['qmap'] = f'qmap/{config_fname}'

    if container in ['rtp-pipeline']:
        pipeline_json_keys = ['anatomical', 'bval', 'bvec', 'dwi', 'fs']
        pipeline_json_val = [
            'anatomical/T1.nii.gz', 'bval/dwi.bval',
            'bvec/dwi.bvec', 'dwi/dwi.nii.gz', 'fs/fs.zip',
        ]
        config_json_dict[container] = {
            key: value for key,
            value in zip(pipeline_json_keys, pipeline_json_val)
        }

        tractparams = lc_config['container_specific'][container]['tractparams']
        if tractparams:
            config_fname = copy_configs(
                container, tractparams,
                analysis_dir, force, 'tractparams',
            )
            config_json_dict[container]['tractparams'] = f'tractparams/{config_fname}'

    if container in ['rtp2-pipeline']:
        pipeline_json_keys = ['anatomical', 'bval', 'bvec', 'dwi', 'fs']
        pipeline_json_val = [
            'anatomical/T1.nii.gz', 'bval/dwi.bval',
            'bvec/dwi.bvec', 'dwi/dwi.nii.gz', 'fs/fs.zip',
        ]
        config_json_dict[container] = {
            key: value for key,
            value in zip(pipeline_json_keys, pipeline_json_val)
        }
        tractparams = lc_config['container_specific'][container]['tractparams']
        fsmask = lc_config['container_specific'][container]['fsmask']
        use_qmap = lc_config['container_specific'][container]['use_qmap']
        if tractparams:
            config_fname = copy_configs(
                container, tractparams,
                analysis_dir, force, 'tractparams',
            )
            config_json_dict[container]['tractparams'] = f'tractparams/{config_fname}'
        if fsmask:
            config_fname = copy_configs(container, fsmask, analysis_dir, 'fsmask')
            config_json_dict[container]['fsmask'] = f'fsmask/{config_fname}'
        if use_qmap:
            config_fname = 'qmap.zip'
            config_json_dict[container]['qmap'] = f'qmap/{config_fname}'

    return config_json_dict


def gen_config_json_extra_field(container, config_json_dict):

    config_dict = config_json_dict[container]

    extra_field_config_json = {}
    for key in config_dict.keys():
        extra_field_config_json[key] = {
            'location': {
                'path': op.join('/flywheel/v0/input', config_dict[key]),
                'name': op.basename(config_dict[key]),
            },
            'base': 'file',
        }
    # this is check for anatrois and freesurferator
    if 'anat' in extra_field_config_json.keys() and 'pre_fs' in extra_field_config_json.keys():
        del extra_field_config_json['anat']

    return extra_field_config_json


def write_json(extra_field_config_json, json_path, force):
    # 1) Load safely
    with open(json_path) as infile:
        config = json.load(infile)

    # 2) Decide whether to set/overwrite
    if 'inputs' not in config:
        logger.info(f"'inputs' field missing; adding to {json_path}")
        config['inputs'] = extra_field_config_json

    elif force:
        logger.info(f"'inputs' already exists in {json_path}; overwriting because --force=True")
        config['inputs'] = extra_field_config_json

    else:
        logger.warning(
            f"'{json_path}' already has an 'inputs' field; use --force to overwrite",
        )
        # No change to config

    # 3) Write back (only really changed if we set above)
    with open(json_path, 'w') as outfile:
        json.dump(config, outfile, indent=4)

    return True


def copy_and_edit_config_json(parser_namespace):
    '''
    This function is used to automatically read config.yaml
    and get the input file info and put them in the config.json

    '''

    # get the config json dict and copy the extra configs
    config_json_dict = gen_config_dict_and_copy(parser_namespace)

    # read the yaml to get input info
    lc_config_fpath = parser_namespace.lc_config
    lc_config = lc_config = do.read_yaml(lc_config_fpath)
    logger.info('\n copy_and_edit_config_json reading lc config yaml')
    # read parameters from lc_config
    container = lc_config['general']['container']
    force = lc_config['general']['force']

    # get the path of the json under analysis dir
    json_under_analysis_dir = config_json_dict['config_path']

    # get the extra field for config json
    extra_field_config_json = gen_config_json_extra_field(container, config_json_dict)

    json_under_analysis_dir = config_json_dict['config_path']

    if write_json(extra_field_config_json, json_under_analysis_dir, force):
        logger.info(f'Successfully write json for {container}')

    return config_json_dict


def prepare_dwi(parser_namespace, df_subses, layout):
    """
    This is the major function for doing the preparation, it is doing the work
    1. write the config.json (analysis level)
    2. create symlink for input files (subject level)

    Parameters
    ----------
    lc_config : TYPE
        DESCRIPTION.
    df_subses : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    # read the yaml to get input info
    lc_config_fpath = parser_namespace.lc_config
    lc_config = lc_config = do.read_yaml(lc_config_fpath)
    logger.info('\n prepare_dwi_input_folder reading lc config yaml')

    container = lc_config['general']['container']
    force = lc_config['general']['force']
    version = lc_config['container_specific'][container]['version']

    # read parameters from lc_config
    basedir = lc_config['general']['basedir']
    container = lc_config['general']['container']
    force = lc_config['general']['force']
    analysis_name = lc_config['general']['analysis_name']
    version = lc_config['container_specific'][container]['version']
    bidsdir_name = lc_config['general']['bidsdir_name']
    container_folder = op.join(
        basedir,
        bidsdir_name,
        'derivatives',
        f'{container}_{version}',
    )
    analysis_dir = op.join(container_folder, f'analysis-{analysis_name}')

    logger.info(
        '#####################################################\n'
        + 'Preparing for DWI pipeline RTP2',
    )

    # copy and edit config json and extra config files
    config_json_dict = copy_and_edit_config_json(parser_namespace)

    if config_json_dict:
        logger.info(
            '#####################################################\n'
            + 'Successfully copy extra confings and rewrite the json\n',
        )
    else:
        logger.error(
            '\n'
            + '#####################################################\n'
            + 'Prepare json not finished. Please check\n',
        )
        raise Exception(
            'Sorry the Json file seems not being written correctly, \
                it may cause container dysfunction',
        )

    logger.info(
        '\n'
        + '#####################################################\n'
        + f'DWI Create the symlinks of all the input files RTP2-{container}\n',
    )

    for row in df_subses.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi

        logger.info(
            '\n'
            + 'The current ses is: \n'
            + f'{sub}_{ses}_{container}_{version}\n',
        )

        if RUN == 'True' and dwi == 'True':

            tmpdir = op.join(
                analysis_dir,
                'sub-' + sub,
                'ses-' + ses,
                'output', 'tmp',
            )
            container_logdir = op.join(
                analysis_dir,
                'sub-' + sub,
                'ses-' + ses,
                'log',
            )
            # For all the container, create ses-/log and ses-/output/tmp
            # if we will use 1 session anatrois/freesurferator as ref,
            # we will not creat outoput dir for other session

            if container not in ['anatrois', 'freesurferator']:
                os.makedirs(tmpdir, exist_ok=True)
                os.makedirs(container_logdir, exist_ok=True)
            else:
                use_src_session = lc_config['container_specific'][container]['use_src_session']
                if use_src_session is not None:
                    current_session_dir = op.join(analysis_dir, 'sub-' + sub, 'ses-' + ses)
                    src_session_dir = op.join(analysis_dir, 'sub-' + sub, 'ses-' + use_src_session)
                    if ses != use_src_session and \
                            (
                                os.path.islink(current_session_dir)
                                or os.path.exists(src_session_dir)
                            ):
                        logger.warning(
                            f'\n You are preparing for the session:{ses} that are'
                            + 'not the reference session:{use_src_session}',
                        )
                        logger.warning('\n Not creating tmp dir, skip')
                else:
                    os.makedirs(tmpdir, exist_ok=True)
                    os.makedirs(container_logdir, exist_ok=True)
            try:
                do.copy_file(
                    parser_namespace.lc_config,
                    op.join(container_logdir, 'lc_config.yaml'),
                    force,
                )
                config_json_path = config_json_dict['config_path']
                do.copy_file(config_json_path, op.join(container_logdir, 'config.json'), force)
            except Exception:
                logger.error(f'\n copy config file and create tmp failed for sub-{sub}_ses-{ses}')

            if container in ['rtppreproc' , 'rtp2-preproc']:
                prep_dwi.rtppreproc(
                    config_json_dict, analysis_dir,
                    lc_config, sub, ses, layout,
                )
            elif container in ['rtp-pipeline', 'rtp2-pipeline']:
                prep_dwi.rtppipeline(
                    config_json_dict, analysis_dir,
                    lc_config, sub, ses,
                )
            elif container in ['anatrois', 'freesurferator']:
                prep_dwi.anatrois(
                    config_json_dict, analysis_dir,
                    lc_config, sub, ses, layout,
                )
            else:
                logger.error(
                    f'\n{container} is not created, check for typos or \
                    contact admin for singularity images\n',
                )
        else:
            continue
    logger.info(
        '\n'
        + '#####################################################\n',
    )
    return True
