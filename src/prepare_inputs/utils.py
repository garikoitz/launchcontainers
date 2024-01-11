"""
MIT License

Copyright (c) 2020-2023 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2023 Leandro Lecca
Copyright (c) 2022-2023 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""

import argparse
from argparse import RawDescriptionHelpFormatter
import yaml
from yaml.loader import SafeLoader
import logging
import os
import shutil
import sys
import pandas as pd

logger=logging.getLogger("GENERAL")


# %% parser
def get_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description= """
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
    , formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        "-lcc",
        "--lc_config",
        type=str,
        #default="",
        help="path to the config file",
    )
    parser.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        #default="",
        help="path to the subSesList",
    )
    parser.add_argument(
        "-cc",
        "--container_specific_config",
        nargs='*',
        default=[],
        #default=["/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/BIDS/config.json"],
        help="path to the container specific config file(s). First file needs to be the config.json file of the container. \
        Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). \
        some don't need any configs (e.g fmriprep)    Add them here separated with a space.",
    )
   
    parser.add_argument('--run_lc', action='store_true',
                        help= "if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this argument empty. \
                        then the launchcontainer.py will prepare \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run"
                        )
    
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode",
                         )
    parser.add_argument(
        "--DEBUG",
        action="store_true",
        help="if you want to find out what is happening of particular step, this will print you more detailed information",
                         )    
    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    parse_dict = vars(parser.parse_args())
    parse_namespace= parser.parse_args()
    
    logger.info("\n"+
        "#####################################################\n" +
        "This is the result from get_parser()\n"+
                f'{parse_dict}\n'+    
        "#####################################################\n")
    
    return parse_namespace
# %% read yaml
def read_yaml(path_to_config_file):
    """
    Input:
    the path to the config file

    Returns
    a dictionary that contains all the config info

    """
    with open(path_to_config_file, "r") as v:
        config = yaml.load(v, Loader=SafeLoader)

    container = config["general"]["container"]

    logger.warning("\n"+
            "#####################################################\n"
            +f"Successfully read the config file {path_to_config_file} \n"
            +f'Basedir is: {config["general"]["basedir"]} \n'
            +f'Container is: {container}_{config["container_specific"][container]["version"]} \n'
            +f'Host is: {config["general"]["host"]} \n'
            +f'analysis folder is: {config["general"]["analysis_name"]} \n'
            +f"##################################################### \n")

    return config

# %% function to read subSesList. txt
def read_df(path_to_df_file):
    """
    Input:
    path to the subject and session list txt file

    Returns
    a dataframe

    """
    outputdf = pd.read_csv(path_to_df_file, sep=',',dtype=str)
    num_rows = len(outputdf)

    # Print the result    
    logger.info("\n"+
        "#####################################################\n" +
        f"The dataframe{path_to_df_file} is successfully read\n"+
                f"The DataFrame has {num_rows} rows \n"+    
        "#####################################################\n")

    return outputdf

#%% function setup_logger
def setup_logger():
    # instantiate logger
    logger=logging.getLogger()
    # define handler and formatter
    handler = logging.StreamHandler() #TODO: this should dbe implement to filehandler also , so that we have lc logs
    formatter= logging.Formatter("%(asctime)s %(levelname)8s  %(module)8s:%(funcName)s:%(lineno)d %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    # add formatter to handler
    handler.setFormatter(formatter)
    # add handler to logger
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)

    return logger
#%% copy file
def copy_file(src_file, dst_file, force):
    logger.info("\n"+
        "#####################################################\n")
    if not os.path.isfile(src_file):
        logger.error(" An error occurred")
        raise FileExistsError("the source file is not here")
    
    logger.info("\n"+
                f"---start copying {src_file} to {dst_file} \n")
    try:
        if ((not os.path.isfile(dst_file)) or (force)) or (os.path.isfile(dst_file) and force):
            shutil.copy(src_file, dst_file)
            logger.info("\n"+
                f"---{src_file} has been successfully copied to {os.path.dirname(src_file)} directory \n"+
                f"---REMEMBER TO CHECK/EDIT TO HAVE THE CORRECT PARAMETERS IN THE FILE\n"
            )
        elif os.path.isfile(dst_file) and not force:
            logger.warning("\n"+
                    f"---copy are not operating, the {src_file} already exist")


    # If source and destination are same
    except shutil.SameFileError:
        logger.error("***Source and destination represents the same file.\n")
        raise
    # If there is any permission issue
    except PermissionError:
        logger.error("***Permission denied.\n")
        raise
    # For other errors
    except:
        logger.error("***Error occurred while copying file\n")
        raise
    logger.info("\n"+
        "#####################################################\n")
    
    return dst_file



    