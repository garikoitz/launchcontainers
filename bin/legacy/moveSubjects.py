import argparse
import os
import shutil as sh
import glob 
import subprocess as sp
import numpy as np
import pip
import pandas as pd
import json
import shutil


parser = argparse.ArgumentParser(
    description='''createSymLinks.py 'pathTo/config_launchcontainers.json' ''')
#
# # Required positional argument
parser.add_argument('configFile', type=str, help='path to the config file')
#
args = parser.parse_args()
print('Read config file: ')
print(args.configFile)


# read the variables from json file:
with open(args.configFile, 'r') as v:
    vars = json.load(v)

basedir = vars["config"]["basedir"]
print('Basedir: ')
print(basedir)
rpe = vars["config"]["rpe"]
# THIS ANALYSIS
# tool   ="fs_7.1.1-03d"
# tool   ="rtppreproc_1.1.3"
tool = vars["config"]["tool"]
analysis = vars["config"]["analysis"]


annotfile     = vars["config"]["annotfile"]
mniroizip     = vars["config"]["mniroizip"]
pre_fs        = vars["config"]["pre_fs"]
prefs_zipname = vars["config"]["prefs_zipname"]


# PREVIOUS ANALYSIS
pretoolfs = vars["config"]["pretoolfs"]
preanalysisfs = vars["config"]["preanalysisfs"]

pretoolpp = vars["config"]["pretoolpp"]
preanalysispp = vars["config"]["preanalysispp"]

# Get the unique list of subjects and sessions
codedir = vars["config"]["codedir"]
subseslist = os.path.join(basedir, 'Nifti', "subSesList.txt")

os.chdir(os.path.join(basedir,'Nifti'))

# READ THE FILE
dt = pd.read_csv(subseslist, sep=",", header=0)
for index in dt.index:
    sub  = dt.loc[index, 'sub']
    ses  = dt.loc[index, 'ses']
    RUN  = dt.loc[index, 'RUN']
    dwi  = dt.loc[index, 'dwi']
    func = dt.loc[index, 'func']

    if RUN:
        # Now just move from /scratch to /dipc
        fs = pretoolfs
        pp = pretoolpp
        pi = tool
        print(sub,ses,fs,pp,pi)
        
        # dipcf = os.path.join('/dipc/glerma/DATA/MINI/Nifti/derivatives')
        # derif = os.path.join(basedir,'Nifti','derivatives')
        dipcf = os.path.join('/scratch/glerma/DATA/MINI/Nifti/derivatives')
        derif = os.path.join('/dipc/glerma/DATA/MINI/Nifti','derivatives')

        srcfs = os.path.join(derif,fs,'analysis-01','sub-'+sub,'ses-'+ses)
        srcpp = os.path.join(derif,pp,'analysis-01','sub-'+sub,'ses-'+ses)
        srcpi = os.path.join(derif,pi,'analysis-01','sub-'+sub,'ses-'+ses)
        #
        dstfs = os.path.join(dipcf,fs,'analysis-01','sub-'+sub,'ses-'+ses)
        dstpp = os.path.join(dipcf,pp,'analysis-01','sub-'+sub,'ses-'+ses)
        dstpi = os.path.join(dipcf,pi,'analysis-01','sub-'+sub,'ses-'+ses)

        if os.path.isdir(srcfs):
            # sh.move(srcfs,dstfs)
            sh.copytree(srcfs,dstfs,symlinks=True)
        if os.path.isdir(srcpp):
            # sh.move(srcpp,dstpp)
            sh.copytree(srcpp,dstpp,symlinks=True)
        if os.path.isdir(srcpi):
            # sh.move(srcpi,dstpi)
            sh.copytree(srcpi,dstpi,symlinks=True)
        

os.chdir(os.path.join(basedir,'Nifti'))

