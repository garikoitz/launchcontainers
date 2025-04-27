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

import argparse
import sys
from argparse import RawDescriptionHelpFormatter

from launchcontainers.other_cli_tool.zip_example_config import do_zip_configs

# %% parser


def get_parser():
    """Parses command line inputs
    Args:
        None
    Returns:
        parse_namespace(argparse.Namespace): dict-like storing the command line arguments

    """
    parser = argparse.ArgumentParser(
        description="""
        This python program helps you analysis MRI data through different containers,
        Before you make use of this program, please prepare the environment, edit the required config files, to match your analysis demand. \n
        SAMPLE CMD LINE COMMAND \n\n
        ###########STEP1############# \n
        To begin the analysis, you need to first prepare and check the input files by typing this command in your bash prompt:
        python path/to/the/launchcontianer.py -lcc path/to/launchcontainer_config.yaml -ssl path/to/subject_session_info.txt
        -cc path/to/container_specific_config.json \n
        ##--cc note, for the case of rtp-pipeline, you need to input two paths, one for config.json and one for tractparm.csv \n\n
        ###########STEP2############# \n
        After you have done step 1, all the config files are copied to BIDS/sub/ses/analysis/ directory
        When you are confident everything is there, press up arrow to recall the command in STEP 1, and just add --run_lc after it. \n\n

        We add lots of check in the script to avoid program breakdowns. if you found new bugs while running, do not hesitate to contact us"""
        , formatter_class=RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '-lcc',
        '--lc_config',
        type=str,
        # default="",
        help='path to the config file',
    )
    parser.add_argument(
        '-ssl',
        '--sub_ses_list',
        type=str,
        # default="",
        help='path to the subSesList',
    )
    parser.add_argument(
        '-cc',
        '--container_specific_config',
        nargs='*',
        default=[],
        # default=["/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/BIDS/config.json"],
        help="path to the container specific config file(s). First file needs to be the config.json file of the container. \
        Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). \
        some don't need any configs (e.g fmriprep)    Add them here separated with a space.",
    )

    parser.add_argument(
        '--run_lc', action='store_true',
        help='if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this argument empty. \
                        then the launchcontainer.py will prepare \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run',
    )

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode',
    )
    parser.add_argument(
        '--DEBUG',
        action='store_true',
        help='if you want to find out what is happening of particular step, this will print you more detailed information',
    )
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

    print(
        '\n'
        + '#####################################################\n'
        + 'This is the result from get_parser()\n'
        + f'{parse_dict}\n'
        + '#####################################################\n',
    )

    return parse_namespace

# %% parser


def get_parser2():
    """Parses command line inputs
    Args:
        None
    Returns:
        parse_namespace(argparse.Namespace): dict-like storing the command line arguments
        parse_dict(mappingproxy): parsed arguments from the argument parser
    """
    parser = argparse.ArgumentParser(
        description="""
        This python program helps you analysis MRI data through different containers,
        Before you make use of this program, please edit the required config files to match your analysis demand. \n
        SAMPLE CMD LINE COMMAND \n\n
        ###########STEP1############# \n
        To begin the analysis, you need to first prepare and check the input files by typing this command in your bash prompt:
        python path/to/the/launchcontianer.py -lcc path/to/launchcontainer_config.yaml -ssl path/to/subject_session_info.txt
        -cc path/to/contianer_specific_config.json \n
        ##--cc note, for the case of rtp-pipeline, you need to input two paths, one for config.json and one for tractparm.csv \n\n
        ###########STEP2############# \n
        After you have done step 1, all the config files are copied to nifti/sub/ses/analysis/ directory
        When you are confident everthing is there, press up arrow to recall the command in STEP 1, and just add --run_lc after it. \n\n

        We add lots of check in the script to avoid program breakdowns. if you found new bugs while running, do not hesitate to contact us"""
        , formatter_class=RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '-lcc',
        '--lc_config',
        type=str,
        # default="/Users/tiger/TESTDATA/PROJ01/nifti/config_launchcontainer_copy.yaml",
        # default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/lc_config.yaml",
        help='path to the config file',
    )
    parser.add_argument(
        '-ssl',
        '--sub_ses_list',
        type=str,
        # default="/Users/tiger/TESTDATA/PROJ01/nifti/subSesList.txt",
        # default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/subSesList.txt",
        help='path to the subSesList',
    )
    parser.add_argument(
        '-cc',
        '--container_specific_config',
        nargs='+',
        # default="/Users/tiger/Documents/GitHub/launchcontainers/example_configs/container_especific_example_configs/anatrois/4.2.7_7.1.1/example_config.json",
        # default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/config.json",
        help='path to the container specific config file(s). First file needs to be the config.json file of the container. Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). Add them here separated with a space.',
    )

    parser.add_argument(
        '--run_lc', action='store_true',
        help='if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this arguement empty. \
                        then the launchcontainer.py will preapre \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run',
    )
    parser.add_argument('--not_run_lc', dest='run_lc', action='store_false')
    # parser.set_defaults(run_lc=False)

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode',
    )

    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

    print(
        '\n'
        + '#####################################################\n'
        + 'This is the result from get_parser()\n'
        + f'{parse_dict}\n'
        + '#####################################################\n',
    )

    return parse_namespace, parse_dict


def get_parser_2subcommand():
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

    # get the path from command line input
    # parser_namespace = get_parser()
    parser_namespace, parse_dict = get_parser_2subcommand()
    print('\n this is namespace')
    print(parser_namespace)

    try:
        print('\n this is namespace container_specific_config')
        print(parser_namespace.container_specific_config)

        lc_config_path = parser_namespace.lc_config
        print(f'\n this is lc_config_path: {lc_config_path}')
    except Exception:
        pass
    print('\n this is parse_dict')
    # the code to really launch the function
    print(parse_dict)
    parser_namespace.func(parser_namespace)


# #%%
if __name__ == '__main__':
    main()
