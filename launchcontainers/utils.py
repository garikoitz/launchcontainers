# """
# MIT License
# Copyright (c) 2020-2025 Garikoitz Lerma-Usabiaga
# Copyright (c) 2020-2022 Mengxing Liu
# Copyright (c) 2022-2023 Leandro Lecca
# Copyright (c) 2022-2025 Yongning Lei
# Copyright (c) 2023 David Linhardt
# Copyright (c) 2023 Iñigo Tellaetxe
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """
from __future__ import annotations

import logging
import os
import os.path as op
import shutil
import sys
from os import makedirs

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
    outputdf = pd.read_csv(path_to_df_file, sep=',', dtype=str)
    try:
        num_of_true_run = len(outputdf.loc[outputdf['RUN'] == 'True'])
    except Exception as e:
        num_of_true_run = None
        logger.warn(f'The df you are reading is not subseslist \
            or something is wrong {e}')
    logger.info(outputdf.head(5))

    return outputdf, num_of_true_run


def setup_logger(print_command_only, verbose=False, debug=False, log_dir=None, log_filename=None):
    '''
    stream_handler_level: str,  optional
        if no input, it will be default at INFO level, \
            this will be the setting for the command line logging

    verbose: bool, optional
    debug: bool, optional
    log_dir: str, optional
        if no input, there will have nothing to be saved \
            in log file but only the command line output

    log_filename: str, optional
        the name of your log_file.

    '''
    # set up the lowest level for the logger first, so that all the info will be get
    logger.setLevel(logging.DEBUG)

    # set up formatter and handler so that the logging info can go to stream or log files
    # with specific format
    log_formatter = logging.Formatter(
        '%(asctime)s (%(name)s):[%(levelname)s] \
            %(module)s - %(funcName)s() - line:%(lineno)d   $ %(message)s ',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    stream_formatter = logging.Formatter(
        '(%(name)s):[%(levelname)s]  %(module)s:%(funcName)s:%(lineno)d %(message)s',
    )
    # Define handler and formatter
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stream_formatter)
    if verbose:
        stream_handler.setLevel(logging.INFO)
    elif print_command_only:
        stream_handler.setLevel(logging.CRITICAL)
    elif debug:
        stream_handler.setLevel(logging.DEBUG)
    else:
        stream_handler.setLevel(logging.WARNING)
    logger.addHandler(stream_handler)

    if log_dir:
        if not os.path.isdir(log_dir):
            makedirs(log_dir)

        file_handler_info = (
            logging.FileHandler(op.join(log_dir, f'{log_filename}_info.log'), mode='a')
        )
        file_handler_error = (
            logging.FileHandler(op.join(log_dir, f'{log_filename}_error.log'), mode='a')
        )
        file_handler_info.setFormatter(log_formatter)
        file_handler_error.setFormatter(log_formatter)

        file_handler_info.setLevel(logging.INFO)
        file_handler_error.setLevel(logging.ERROR)
        logger.addHandler(file_handler_info)
        logger.addHandler(file_handler_error)

    return logger


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
