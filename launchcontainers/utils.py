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
import errno

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
        logger.warn(f'The df you are reading is not subseslist'
           +f'or something is wrong {e}')
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

def force_symlink(file1, file2, force):
    """Creates symlinks making sure
    Args:
        file1 (str): path to the source file,
        which is the output of the previous container
        file2 (str): path to the destination file, which is the input of the current container
        force (bool): specifies if existing files will be rewritten or not.
        Set in the config.yaml file.

    Raises:
        n (OSError): Raised if input file does not exist when
        trying to create a symlink between file1 and file2
        e (OSError):
        e: _description_
    """
    logger.info(
        '\n'
        + '-----------------------------------------------\n',
    )
    # If force is set to False (we do not want to overwrite)
    if not force:
        try:
            # Try the command, if the files are correct and the symlink does not exist, create one
            logger.info(
                '\n'
                + f'---creating symlink for source file: {file1} and destination file: {file2}\n',
            )
            os.symlink(file1, file2)
            logger.info(
                '\n'
                + f'--- force is {force}, \
                -----------------creating success -----------------------\n',
            )
        # If raise [erron 2]: file does not exist, print the error and pass
        except OSError as n:
            if n.errno == 2:
                logger.error(
                    '\n'
                    + 'Input files are missing, please check \n',
                )
                pass
            # If raise [errno 17] the symlink exist,
            # we don't force and print that we keep the original one
            elif n.errno == errno.EEXIST:
                logger.warning(
                    '\n'
                    + f'--- force is {force}, symlink exist, remain old \n',
                )
            else:
                logger.error('\n' + 'Unknown error, break the program')
                raise n

    # If we set force to True (we want to overwrite)
    if force:
        try:
            # Try the command, if the file are correct and symlink not exist, it will create one
            os.symlink(file1, file2)
            logger.info(
                '\n'
                + f'--- force is {force}, symlink empty, new link created successfully\n ',
            )
        # If the symlink exists, OSError will be raised
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(file2)
                logger.warning(
                    '\n'
                    + '--- overwriting the existing symlink',
                )
                os.symlink(file1, file2)
                logger.info(
                    '\n'
                    + '----------------- Overwrite success -----------------------\n',
                )
            elif e.errno == 2:
                logger.error(
                    '\n'
                    + '***input files are missing, please check that they exist\n',
                )
                raise e
            else:
                logger.error(
                    '\n'
                    + '***ERROR***\n'
                    + 'We do not know what happened\n',
                )
                raise e
    check_symlink(file2)
    logger.info(
        '\n'
        + '-----------------------------------------------\n',
    )
    return

def check_symlink(path: str) -> None:
    """
    Function to check if a symlink is a link and also if it is being pointed to correct place

    if not point to a real place, the prepare mode will fail

    """
    if op.islink(path):
        if op.exists(path):
            logger.info(
                ' √ Symlink %r is valid and points to %r',
                path, op.realpath(path),
            )
        else:
            target = os.readlink(path)
            logger.error(
                'X Symlink %r is broken (target %r not found)',
                path, target,
            )
            raise FileNotFoundError(f'Broken symlink: {path!r} → {target!r}')

    else:
        logger.info(' %r is not a symlink', path)