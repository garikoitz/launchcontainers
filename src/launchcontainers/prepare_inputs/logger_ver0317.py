import requests
import argparse
from argparse import RawDescriptionHelpFormatter
import yaml
from yaml.loader import SafeLoader
import logging
import os
import shutil
import sys
import pandas as pd
import os.path as op
from os import makedirs
logger = logging.getLogger("Launchcontainers")
def setup_logger(print_command_only, verbose=False, debug=False, log_dir=None, log_filename=None):
    '''
    stream_handler_level: str,  optional
        if no input, it will be default at INFO level, this will be the setting for the command line logging

    verbose: bool, optional
    debug: bool, optional
    log_dir: str, optional
        if no input, there will have nothing to be saved in log file but only the command line output

    log_filename: str, optional
        the name of your log_file.

    '''
    # set up the lowest level for the logger first, so that all the info will be get
    logger.setLevel(logging.DEBUG)
    

    # set up formatter and handler so that the logging info can go to stream or log files 
    # with specific format
    log_formatter = logging.Formatter(
        "%(asctime)s (%(name)s):[%(levelname)s] %(module)s - %(funcName)s() - line:%(lineno)d   $ %(message)s ",
        datefmt="%Y-%m-%d %H:%M:%S",
    )    

    stream_formatter = logging.Formatter(
        "(%(name)s):[%(levelname)s]  %(module)s:%(funcName)s:%(lineno)d %(message)s"
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