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


def get_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description="""
        This python program helps you analysis MRI data through different containers,
        Before you make use of this program, please prepare the environment, \
        edit the required config files, to match your analysis demand. \n

        SAMPLE CMD LINE COMMAND \n\n
        ###########STEP1############# \n
        To begin the analysis, you need to first prepare and check the input files
        by typing this command in your bash prompt:
        mrilc -lcc path/to/launchcontainer_config.yaml -ssl path/to/subject_session_info.txt
        -cc path/to/container_specific_config.json \n
        ##--cc note, for the case of rtp-pipeline, you need to input two paths,
        # one for config.json and one for tractparm.csv \n\n
        ###########STEP2############# \n
        After you have done step 1, all the config files are copied to
        BIDS/sub/ses/analysis/ directory
        When you are confident everything is there, press up arrow to
        recall the command in STEP 1, and just add --run_lc after it. \n\n

        We add lots of check in the script to avoid program breakdowns.
        if you found new bugs while running, do not hesitate to contact us \n
        For developer To zip all the configs into package simply type zip_configs\n
        For tester/developer: if you want to fake a container and it's analysis folder type do \n
        createbids -cbc fake_bids_dir.yaml -ssl subSesList.txt \n""",
        formatter_class=RawDescriptionHelpFormatter,
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
        type=str,
        help='path to the container specific \
         config file, \
        it stores the parameters for the container.',
    )
    parser.add_argument(
        '--copy_configs',
        type=str,
        help='Path to copy the configs, usually your working directory',
    )
    parser.add_argument(
        '--run_lc',
        action='store_true',
        help='if you type --run_lc, the entire program will be launched, jobs will be send to \
                cluster and launch the corresponding container you suggest in config_lc.yaml. \
                We suggest that the first time you run launchcontainer.py, \
                leave this argument empty \
                then the launchcontainer.py will prepare \
                all the input files for you and print the command you want \
                to send to container, after you \
                check all the configurations are correct and ready, \
                you type --run_lc to make it run',
    )

    # parser.add_argument(
    #     "--quite",
    #     action="store_true",
    #     help="if you want to open quite mode, type --quite,
    #     then it will print you only the warning level ",
    # )
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


def get_create_bids_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description="""
        #########This function is for create a fake bids format container analysis dir""",
        formatter_class=RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '-cbc',
        '--creat_bids_config',
        type=str,
        # default="",
        help='path to the create bids config file',
    )
    parser.add_argument(
        '-ssl',
        '--sub_ses_list',
        type=str,
        # default="",
        help='path to the subSesList',
    )
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

    return parse_namespace, parse_dict
