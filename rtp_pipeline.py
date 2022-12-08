import argparse
from logging import DEBUG
import os
import shutil as sh
import glob
import subprocess as sp
from xxlimited import Str
import numpy as np
import pandas as pd
import json

# *** OLD delete
# def _get_parser()
# parser = argparse.ArgumentParser()
# # # Required positional argument
# parser.add_argument('configFile', type=str, help='path to the config file')
# args = parser.parse_args()
# print('Read config file: ')
# print(args.configFile)
# *** end OLD delete

def _get_parser():
    """
    Parse command line inputs for this function.
    Returns
    -------
    parser.parse_args() : argparse dict
    Notes
    -----
    # Argument parser follow template provided by RalphyZ.
    # https://stackoverflow.com/a/43456577
    """
    parser = argparse.ArgumentParser()
    optional = parser._action_groups.pop()
    required = parser.add_argument_group("Required Argument:")
    required.add_argument(
        "--list",
        dest="list_path",
        type=str,
        help="The path to the subject list (.txt). ",
        required=True,
    )
    required.add_argument(
        "--config",
        dest="config_path",
        type=str,
        help="The path to the configuration file. ",
        required=True,
    )
    optional.add_argument(
        "--tmp",
        dest="tmp_path",
        type=str,
        help="The path to the temporary directory. ",
        default="~/tmp/"
    )
    optional.add_argument(
        "--log",
        dest="log_path",
        type=str,
        help="The path to the log directory. ",
        default="~/log/"
    )


def rtp_pipeline(
    list_path,
    config_path,
    tmp_path,
    log_path,
):
    """rtp_pipeline is a Python programm for the RTP tractography and metrics pipeline.
    Parameters
    ----------
    list_path : str
        The path to the subject list (.txt).
    config_path : str
        The path to the configuration file.
    """


    # If tmpdir and logdir do not exist, create them
    if not os.path.isdir(tmp_path): os.mkdir(tmp_path)
    if not os.path.isdir(log_path): os.mkdir(log_path)

    # Get the unique list of subjects and sessions
    
    #*** OLD delete
    #subseslist=os.path.join(basedir,"Nifti","subSesList.txt")
    #*** end OLD delete

    subseslist=os.path.join(list_path)

    # READ THE FILE
    dt = pd.read_csv(subseslist, sep=",", header=0)

    for row in dt.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
        func = row.func
        if RUN and dwi:
            cmdstr = (f"{codedir}/qsub_generic.sh " +
                    f"-t {tool} " +
                    f"-s {sub} " +
                    f"-e {ses} " +
                    f"-a {analysis} " +
                    f"-b {basedir} " +
                    f"-o {codedir} " +
                    f"-m {mem} " +
                    f"-q {que} " +
                    f"-c {core} " +
                    f"-p {tmpdir} " +
                    f"-g {logdir} " +
                    f"-i {sin_ver} " +
                    f"-n {container} " +
                    f"-u {qsub} " +
                    f"-h {host} " +
                    f"-d {manager} " +
                    f"-f {system} " +
                    f"-j {maxwall} ")
            
            print(cmdstr)
            sp.call(cmdstr, shell=True)

def _main():
    """rtp_pipeline entry point"""
    command_str = "rtp_pipeline " + " ".join(sys.argv[1:])
    options = _get_parser().parse_args()
    pySPFM(**vars(options), command_str=command_str)


if __name__ == "__main__":
    _main()