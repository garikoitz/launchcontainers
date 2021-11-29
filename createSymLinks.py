import argparse
import os
import shutil as sh
import glob 
import subprocess as sp
import numpy as np
import pip
package='nibabel'
# !{sys.executable} -m pip install nibabel  # inside jupyter console
def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package])
import_or_install(package)
import nibabel as nib
import pandas as pd
import json
import shutil


parser = argparse.ArgumentParser(
    description='''createSymLinks.py 'pathTo/config_launchcontainers.json' ''')
#
# # Required positional argument
parser.add_argument('configFile', type=str, help='path to the config file')
# parser.add_argument('wildcard', type=str, help='wildcard to select folders')
# parser.add_argument('fullPathFile', type=str, help='path and file name to write')
# parser.add_argument('separator', type=str, help='separator to use')

# Optional positional argument
# parser.add_argument('opt_pos_arg', type=int, nargs='?',
#                             help='An optional integer positional argument')

# Optional argument
# parser.add_argument('--opt_arg', type=int,
#                             help='An optional integer argument')

# Switch
# parser.add_argument('--switch', action='store_true',
#                             help='A boolean switch')

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
os.chdir(codedir)

# READ THE FILE
dt = pd.read_csv(subseslist, sep=",", header=0)
for index in dt.index:
    sub = dt.loc[index, 'sub']
   # if isinstance(sub.item(),int):
   #     sub=str(sub)

    ses = dt.loc[index, 'ses']
    RUN = dt.loc[index, 'RUN']
    dwi = dt.loc[index, 'dwi']
    func = dt.loc[index, 'func']
    if ('fs' in tool or 'anatrois' in tool) and RUN:
        # Main source dir
        if pre_fs:
            srcAnatPath = os.path.join(basedir,'Nifti','derivatives',pretoolfs,'analysis-'+preanalysisfs,
                                       'sub-'+sub, 'ses-'+ses,'output')
            zips = sorted(glob.glob(os.path.join(srcAnatPath,prefs_zipname+'*')),key=os.path.getmtime)
            try:
                src_anatomical = zips[-1]
            except:
                print(f"{sub} doesn't have pre_fs, skipping")
                continue
        else:
            src_anatomical = os.path.join(
                basedir, 'Nifti', 'sub-'+sub, 'ses-'+ses, 'anat', 'sub-'+sub+'_ses-'+ses+'_T1w.nii.gz')
        
        # Main destination  dir
        dstDirIn = os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                'analysis-'+analysis, 'sub-'+sub, 'ses-'+ses, 'input')
        dstDirOp = os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                'analysis-'+analysis, 'sub-'+sub, 'ses-'+ses, 'output')
        dstDirAn = os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                'analysis-'+analysis)


       # Add two new variables to the config_launchcontainer.json file,
       # with the name and the location of the annot and mniroi zip
       # files
       # If it is an empty string, do nothing, otherwise, copy the file
       # to the analysis folder and create a symlink to the input folder
       # of the subject.





        # Create folders if they do not exist
        if not os.path.exists(dstDirIn):
            os.makedirs(dstDirIn)
        if not os.path.exists(dstDirOp):
            os.makedirs(dstDirOp)
        if pre_fs:
            if not os.path.exists(os.path.join(dstDirIn, "pre_fs")):
                os.makedirs(os.path.join(dstDirIn, "pre_fs"))
        else:
            if not os.path.exists(os.path.join(dstDirIn, "anat")):
                os.makedirs(os.path.join(dstDirIn, "anat"))
        if annotfile:    
            if os.path.isfile(annotfile):
                print('Passed '+annotfile+', copying to '+dstDirAn)
                srcAnnotfile = os.path.join(dstDirAn,'annotfile.zip')
                if os.path.isfile(srcAnnotfile):
                    print(srcAnnotfile+' exists, if you want it new, delete it first')
                else:
                    shutil.copyfile(annotfile,os.path.join(dstDirAn,'annotfile.zip'))
            else:
                print(annotfile + ' does not exist')
            if not os.path.exists(os.path.join(dstDirIn, "annotfile")):
                os.makedirs(os.path.join(dstDirIn, "annotfile"))
        if mniroizip:    
            if os.path.isfile(mniroizip):
                print('Passed '+mniroizip+', copying to '+dstDirAn)
                srcMniroizip = os.path.join(dstDirAn,'mniroizip.zip')
                if os.path.isfile(srcMniroizip):
                    print(srcMniroizip+' exists, if you want it new, delete it first')
                else:
                    shutil.copyfile(mniroizip,os.path.join(dstDirAn,'mniroizip.zip'))
            else:
                print(mniroizip + ' does not exist')
            if not os.path.exists(os.path.join(dstDirIn, "mniroizip")):
                os.makedirs(os.path.join(dstDirIn, "mniroizip"))

        # Create the destination paths
        if pre_fs:
            dstAnatomicalFile = os.path.join(dstDirIn, 'pre_fs',"existingFS.zip")
        else:
            dstAnatomicalFile = os.path.join(dstDirIn, 'anat', "T1.nii.gz")
        dstAnnotfile      = os.path.join(dstDirIn, 'annotfile',"annots.zip")
        dstMniroizip      = os.path.join(dstDirIn, 'mniroizip',"mniroizip.zip")

        # Create the symbolic links
        if os.path.isfile(dstAnatomicalFile):
            os.unlink(dstAnatomicalFile)
        if os.path.isfile(src_anatomical):
            os.symlink(src_anatomical, dstAnatomicalFile)
        else:
            print(src_anatomical + ' does not exist')

        if annotfile:
            if os.path.isfile(dstAnnotfile):
                os.unlink(dstAnnotfile)
            if os.path.isfile(srcAnnotfile):
                os.symlink(srcAnnotfile, dstAnnotfile)
            else:
                print(srcAnnotfile + ' does not exist')
        
        if mniroizip:
            if os.path.isfile(dstMniroizip):
                os.unlink(dstMniroizip)
            if os.path.isfile(srcMniroizip):
                os.symlink(srcMniroizip, dstMniroizip)
            else:
                print(srcMniroizip + ' does not exist')


    if 'rtppreproc' in tool and RUN and dwi:
        # Main source dir
        srcDir=os.path.join(basedir, 'Nifti', 'sub-'+sub, 'ses-'+ses)
        # FS source dir
        srcDirfs=os.path.join(basedir, 'Nifti', 'derivatives', pretoolfs,
                                'analysis-'+preanalysisfs, 'sub-'+sub, 'ses-'+ses, 'output')
        # File dirs
        srcT1file=os.path.join(srcDirfs, "T1.nii.gz")
        srcMaskFile=os.path.join(srcDirfs, "brainmask.nii.gz")

        srcDwiF_niiFile=os.path.join(
            srcDir, 'dwi', "sub-"+sub+"_ses-"+ses+"_acq-AP_dwi.nii.gz")
        srcDwiF_bvalFile=os.path.join(
            srcDir, 'dwi', "sub-"+sub+"_ses-"+ses+"_acq-AP_dwi.bval")
        srcDwiF_bvecFile=os.path.join(
            srcDir, 'dwi', "sub-"+sub+"_ses-"+ses+"_acq-AP_dwi.bvec")

        if rpe:
            srcDwiR_niiFile=os.path.join(
                srcDir, 'dwi', "sub-"+sub+"_ses-"+ses+"_acq-PA_dwi.nii.gz")
            srcDwiR_bvalFile=os.path.join(
                srcDir, 'dwi', "sub-"+sub+"_ses-"+ses+"_acq-PA_dwi.bval")
            srcDwiR_bvecFile=os.path.join(
                srcDir, 'dwi', "sub-"+sub+"_ses-"+ses+"_acq-PA_dwi.bvec")

            # If bval and bvec do not exist because it is only b0-s, create them
            # (it would be better if dcm2niix would output them but...)
            img=nib.load(srcDwiR_niiFile)
            volumes=img.shape[3]
            if (not os.path.isfile(srcDwiR_bvalFile)) or (not os.path.isfile(srcDwiR_bvecFile)):
                # Write bval file
                f=open(srcDwiR_bvalFile, "x")
                f.write(volumes * "0 ")
                f.close()

                # Write bvec file
                f=open(srcDwiR_bvecFile, "x")
                f.write(volumes * "0 ")
                f.write("\n")
                f.write(volumes * "0 ")
                f.write("\n")
                f.write(volumes * "0 ")
                f.write("\n")
                f.close()

        # Main destination  dir
        dstDir=os.path.join(basedir, 'Nifti', 'derivatives', tool,
                              'analysis-'+analysis, 'sub-'+sub, 'ses-'+ses, 'input')
        dstDirOp=os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                'analysis-'+analysis, 'sub-'+sub, 'ses-'+ses, 'output')
        # Create folders if they do not exist
        if not os.path.exists(dstDir):
            os.makedirs(dstDir)
        if not os.path.exists(dstDirOp):
            os.makedirs(dstDirOp)

        if not os.path.exists(os.path.join(dstDir, "ANAT")):
            os.makedirs(os.path.join(dstDir, "ANAT"))
        if not os.path.exists(os.path.join(dstDir, "FSMASK")):
            os.makedirs(os.path.join(dstDir, "FSMASK"))
        if not os.path.exists(os.path.join(dstDir, "DIFF")):
            os.makedirs(os.path.join(dstDir, "DIFF"))
        if not os.path.exists(os.path.join(dstDir, "BVAL")):
            os.makedirs(os.path.join(dstDir, "BVAL"))
        if not os.path.exists(os.path.join(dstDir, "BVEC")):
            os.makedirs(os.path.join(dstDir, "BVEC"))
        if rpe:
            if not os.path.exists(os.path.join(dstDir, "RDIF")):
                os.makedirs(os.path.join(dstDir, "RDIF"))
            if not os.path.exists(os.path.join(dstDir, "RBVL")):
                os.makedirs(os.path.join(dstDir, "RBVL"))
            if not os.path.exists(os.path.join(dstDir, "RBVC")):
                os.makedirs(os.path.join(dstDir, "RBVC"))
        # Create the destination paths
        dstT1file=os.path.join(dstDir, 'ANAT', "T1.nii.gz")
        dstMaskFile=os.path.join(dstDir, 'FSMASK', "brainmask.nii.gz")
        dstDwiF_niiFile=os.path.join(dstDir, "DIFF", "dwiF.nii.gz")
        dstDwiF_bvalFile=os.path.join(dstDir, "BVAL", "dwiF.bval")
        dstDwiF_bvecFile=os.path.join(dstDir, "BVEC", "dwiF.bvec")
        if rpe:
            dstDwiR_niiFile=os.path.join(dstDir, "RDIF", "dwiR.nii.gz")
            dstDwiR_bvalFile=os.path.join(dstDir, "RBVL", "dwiR.bval")
            dstDwiR_bvecFile=os.path.join(dstDir, "RBVC", "dwiR.bvec")

        # Create the symbolic links
        os.symlink(srcT1file, dstT1file)
        os.symlink(srcMaskFile, dstMaskFile)
        os.symlink(srcDwiF_niiFile, dstDwiF_niiFile)
        os.symlink(srcDwiF_bvalFile, dstDwiF_bvalFile)
        os.symlink(srcDwiF_bvecFile, dstDwiF_bvecFile)
        if rpe:
            os.symlink(srcDwiR_niiFile, dstDwiR_niiFile)
            os.symlink(srcDwiR_bvalFile, dstDwiR_bvalFile)
            os.symlink(srcDwiR_bvecFile, dstDwiR_bvecFile)

    if 'rtp-pipeline' in tool and RUN and dwi:
        # Main source dir
        srcDirfs=os.path.join(basedir, 'Nifti', 'derivatives', pretoolfs,
                                'analysis-'+preanalysisfs, 'sub-'+sub, 'ses-'+'T01', 'output')
        srcDirpp=os.path.join(basedir, 'Nifti', 'derivatives', pretoolpp,
                                'analysis-'+preanalysispp, 'sub-'+sub, 'ses-'+ses, 'output')

        src_anatomical=os.path.join(srcDirpp, 't1.nii.gz')
        src_fs=os.path.join(srcDirfs, 'fs.zip')
        src_bval=os.path.join(srcDirpp, 'dwi.bvals')
        src_bvec=os.path.join(srcDirpp, 'dwi.bvecs')
        src_dwi=os.path.join(srcDirpp, 'dwi.nii.gz')
        # We want to use the same tractparams.csv for all subjects, define later

        # Main destination  dir
        dstDirIn=os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                'analysis-'+analysis, 'sub-'+sub, 'ses-'+ses, 'input')
        dstDirOp=os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                'analysis-'+analysis, 'sub-'+sub, 'ses-'+ses, 'output')
        # Create folders if they do not exist
        if not os.path.exists(dstDirIn):
            os.makedirs(dstDirIn)
        if not os.path.exists(dstDirOp):
            os.makedirs(dstDirOp)
        if not os.path.exists(os.path.join(dstDirIn, "anatomical")):
            os.makedirs(os.path.join(dstDirIn, "anatomical"))
        if not os.path.exists(os.path.join(dstDirIn, "fs")):
            os.makedirs(os.path.join(dstDirIn, "fs"))
        if not os.path.exists(os.path.join(dstDirIn, "dwi")):
            os.makedirs(os.path.join(dstDirIn, "dwi"))
        if not os.path.exists(os.path.join(dstDirIn, "bval")):
            os.makedirs(os.path.join(dstDirIn, "bval"))
        if not os.path.exists(os.path.join(dstDirIn, "bvec")):
            os.makedirs(os.path.join(dstDirIn, "bvec"))
        if not os.path.exists(os.path.join(dstDirIn, "tractparams")):
            os.makedirs(os.path.join(dstDirIn, "tractparams"))
        # Create the destination paths
        dstAnatomicalFile=os.path.join(dstDirIn, 'anatomical', "T1.nii.gz")
        dstFsfile=os.path.join(dstDirIn, 'fs', "fs.zip")
        dstDwi_dwiFile=os.path.join(dstDirIn, "dwi", "dwi.nii.gz")
        dstDwi_bvalFile=os.path.join(dstDirIn, "bval", "dwi.bval")
        dstDwi_bvecFile=os.path.join(dstDirIn, "bvec", "dwi.bvec")
        dst_tractparams=os.path.join(
            dstDirIn, "tractparams", "tractparams.csv")
        src_tractparams=os.path.join(basedir, 'Nifti', 'derivatives', tool,
                                       'analysis-'+analysis, 'tractparams.csv')

        # Create the symbolic links
        os.symlink(src_anatomical, dstAnatomicalFile)
        os.symlink(src_fs, dstFsfile)
        os.symlink(src_dwi, dstDwi_dwiFile)
        os.symlink(src_bvec, dstDwi_bvecFile)
        os.symlink(src_bval, dstDwi_bvalFile)
        os.symlink(src_tractparams, dst_tractparams)
