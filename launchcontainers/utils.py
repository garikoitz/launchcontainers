# """
# MIT License
# Copyright (c) 2020-2025 Garikoitz Lerma-Usabiaga
# Copyright (c) 2020-2022 Mengxing Liu
# Copyright (c) 2022-2023 Leandro Lecca
# Copyright (c) 2022-2025 Yongning Lei
# Copyright (c) 2023 David Linhardt
# Copyright (c) 2023 Iñigo Tellaetxe
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
from __future__ import annotations

import logging
import os
import os.path as op
import shutil
import sys

import pandas as pd
import yaml
from yaml.loader import SafeLoader


logger = logging.getLogger('Launchcontainers')


def die(*args):
    logger.error(*args)
    sys.exit(1)


def read_yaml(path_to_config_file):
    """
    Input:
    the path to the config file

    Returns
    a dictionary that contains all the config info

    """
    with open(path_to_config_file) as v:
        config = yaml.load(v, Loader=SafeLoader)

    return config


def read_df(path_to_df_file):
    """
    Input:
    path to the subject and session list txt file

    Returns
    a dataframe

    """
    df = pd.read_csv(path_to_df_file, sep=',', dtype=str)
    try:
        num_of_true_run = len(df.loc[df['RUN'] == 'True'])
    except Exception as e:
        num_of_true_run = None
        logger.warn(f'The df you are reading is not subseslist \
            or something is wrong {e}')
    logger.info(df.head(5))

    return df, num_of_true_run


def copy_file(src_file, dst_file, force):
    logger.info('\n' + '#####################################################\n')
    if not os.path.isfile(src_file):
        logger.error(' An error occurred')
        raise FileExistsError('the source file is not here')

    logger.info('\n' + f'---start copying {src_file} to {dst_file} \n')
    try:
        if ((not os.path.isfile(dst_file)) or (force)) or (
            os.path.isfile(dst_file) and force
        ):
            shutil.copy(src_file, dst_file)
            logger.info(
                '\n'
                + f'---{src_file} has been successfully copied to \
                     {os.path.dirname(src_file)} directory \n'
                + '---REMEMBER TO CHECK/EDIT TO HAVE THE CORRECT PARAMETERS IN THE FILE\n',
            )
        elif os.path.isfile(dst_file) and not force:
            logger.warning(
                '\n' + f'---copy are not operating, the {src_file} already exist',
            )

    # If source and destination are the same
    except shutil.SameFileError:
        logger.error('***Source and destination represent the same file.\n')

    # If there is any permission issue, skip it
    except PermissionError:
        logger.warning(f'***Permission denied: {dst_file}. Skipping...\n')

    # For other errors
    except Exception as e:
        logger.error(f'***Error occurred while copying file: {e}\n')

    logger.info('\n' + '#####################################################\n')

    return dst_file


def copy_configs(output_path, force=True):
    # first, know where the tar file is stored
    import pkg_resources

    config_path = pkg_resources.resource_filename('launchcontainers', 'example_configs')

    # second, copy all the files from the source folder to the output_path
    all_cofig_files = os.listdir(config_path)
    for src_fname in all_cofig_files:
        src_file_fullpath = op.join(config_path, src_fname)
        targ_file_fullpath = op.join(output_path, src_fname)
        copy_file(src_file_fullpath, targ_file_fullpath, force)

    return
