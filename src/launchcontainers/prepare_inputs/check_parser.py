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
import sys


# %% parser
def get_parser():
    
    """Parses command line inputs
    Args:
        None
    Returns:
        parse_namespace(argparse.Namespace): dict-like storing the command line arguments

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
    
    print("\n"+
        "#####################################################\n" +
        "This is the result from get_parser()\n"+
                f'{parse_dict}\n'+    
        "#####################################################\n")
    
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
        description= """
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
    , formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        "-lcc",
        "--lc_config",
        type=str,
        # default="/Users/tiger/TESTDATA/PROJ01/nifti/config_launchcontainer_copy.yaml",
        #default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/lc_config.yaml",
        help="path to the config file",
    )
    parser.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        # default="/Users/tiger/TESTDATA/PROJ01/nifti/subSesList.txt",
        #default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/subSesList.txt",
        help="path to the subSesList",
    )
    parser.add_argument(
        "-cc",
        "--container_specific_config",
        nargs='+',
        # default="/Users/tiger/Documents/GitHub/launchcontainers/example_configs/container_especific_example_configs/anatrois/4.2.7_7.1.1/example_config.json",
        #default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/config.json",
        help="path to the container specific config file(s). First file needs to be the config.json file of the container. Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). Add them here separated with a space.",
    )
   
    parser.add_argument('--run_lc', action='store_true',
                        help= "if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this arguement empty. \
                        then the launchcontainer.py will preapre \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run"
                        )
    parser.add_argument('--not_run_lc', dest='run_lc', action='store_false')
    #parser.set_defaults(run_lc=False)
    
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode",
                         )
    
    parse_dict = vars(parser.parse_args())
    parse_namespace= parser.parse_args()

    print("\n"+
        "#####################################################\n" +
        "This is the result from get_parser()\n"+
                f'{parse_dict}\n'+    
        "#####################################################\n")
    
    return parse_namespace, parse_dict   


def main():

    
    #get the path from command line input
    #parser_namespace = get_parser()
    parser_namespace, parse_dict = get_parser2()
    print(parser_namespace.container_specific_config)
    print(parse_dict)

    
# #%%
if __name__ == "__main__":
    main()
