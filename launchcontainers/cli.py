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

import argparse
import logging
import sys
from argparse import RawDescriptionHelpFormatter
from datetime import datetime

from launchcontainers import config_logger
from launchcontainers import do_launch
from launchcontainers import do_prepare
from launchcontainers.other_cli_tool import copy_configs
from launchcontainers.other_cli_tool import create_bids
from launchcontainers.other_cli_tool.zip_example_config import do_zip_configs
logger = logging.getLogger('Launchcontainers')


def get_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        prog='lc',
        description="""
        This python program helps you launch different neuroimaging pipelines on different
        computing clusters to enhance the reproducibility and reliability of data analysis.
        There are 2 main functionality: prepare folder structures and submit jobs.

        If you enter *lc prepare * you are in PREPARE mode, type lc prepare -h for flag help

        If you enter *lc run * you are in RUN mode, type lc run -h for flag help

        we have another 3 command line tools embedded with this software: \n
        createbids: it can help you create a fake bids folder for testing
        TYPE: createbids -h for more help \n

        copyconfigs: it can help you copy the example configs for launching
        TYPE: copyconfigs -h for more help \n
        """,
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--log-dir', '-l',
        type=str,
        default=None,
        help='Directory to write lc.log and dask.log into (default: <workdir>/logs)',
    )
    subparsers = parser.add_subparsers(
        title='utilities',
        dest='mode',
        required=True,
        help='Launchcontainers functionalities',
    )

    # ------------------------
    # lc prepare
    # ------------------------
    prep = subparsers.add_parser(
        'prepare',
        help='Set up analysis folder structure',
    )
    prep.add_argument(
        '-lcc',
        '--lc_config',
        type=str,
        # default="",
        help='path to the config file',
    )
    prep.add_argument(
        '-ssl',
        '--sub_ses_list',
        type=str,
        # default="",
        help='path to the subSesList',
    )
    prep.add_argument(
        '-cc',
        '--container_specific_config',
        type=str,
        help='path to the container specific \
         config file, \
        it stores the parameters for the container.',
    )
    # ------------------------
    # lc run
    # ------------------------
    run = subparsers.add_parser(
        'run',
        help='Validate and submit jobs to cluster',
    )
    run.add_argument(
        '-w',
        '--workdir',
        required=True,
        type=str,
        help='Root of prepared analysis folders',
    )
    run.add_argument(
        '--run_lc',
        action='store_true',
        help='If not input, lc will just print commands without submitting;'
        'if you specify run_lc, it will launch the jobs',
    )
    # ------------------------
    # lc create_bids
    # ------------------------
    create_bids = subparsers.add_parser(
        'create_bids',
        help='Create a fake bids folder based on subseslist and yml',
    )

    create_bids.add_argument(
        '-cbc',
        '--creat_bids_config',
        type=str,
        # default="",
        help='path to the create bids config file',
    )
    create_bids.add_argument(
        '-ssl',
        '--sub_ses_list',
        type=str,
        # default="",
        help='path to the subSesList',
    )
    # ------------------------
    # lc copy_configs
    # ------------------------
    copy_configs = subparsers.add_parser(
        'copy_configs',
        help='Copy example config files to working directory',
    )

    copy_configs.add_argument(
        '-o',
        '--output',
        type=str,
        help='Path to copy the configs, usually your working directory',
    )
    # ------------------------
    # lc zip_configs
    # ------------------------
    zip_configs = subparsers.add_parser(
        'zip_configs',
        help='Archive the example configs and store in the repo (for developer)',
    )

    zip_configs.set_defaults(func=lambda args: do_zip_configs())

    # Other optional arguements for lc

    parser.add_argument(
        '--quiet',
        action='store_true',
        help='if you want to open quiet mode, type --quiet, the the level will be critical',
    )
    # Other optional arguements for lc
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='if you want to open verbose mode, type --verbose, the the level will be info',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='if you want to find out what is happening of particular step, \
            --type debug, this will print you more detailed information',
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

    return parse_namespace, parse_dict


def main():
    parse_namespace, parse_dict = get_parser()
    quiet = parse_namespace.quiet
    verbose = parse_namespace.verbose
    debug = parse_namespace.debug
    logging_dir = parse_namespace.log_dir
    print(f' The logging dir is {logging_dir}')
    # get the dir and fpath for launchcontainer logger
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    logging_fname = f'lc_logger_{timestamp}'
    # set up the logger for prepare mode
    logger = config_logger.setup_logger(quiet, verbose, debug, logging_dir, logging_fname)
    if parse_namespace.mode == 'prepare':
        print('\n....running prepare mode\n')
        do_prepare.main(parse_namespace)

    if parse_namespace.mode == 'run':
        print('\n....running run mode\n')
        do_launch.main(parse_namespace)

    if parse_namespace.mode == 'create_bids':
        create_bids.main()

    if parse_namespace.mode == 'copy_configs':
        copy_configs.main()

    if parse_namespace.mode == 'zip_configs':
        # launch the zip config function
        parse_namespace.func(parse_namespace)


if __name__ == '__main__':
    main()
