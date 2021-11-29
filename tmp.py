import pandas as pd
import os
import subprocess as sp
subSes = "/scratch/lmx/ThaTract/Nifti/subSesList.txt"
subIDs = pd.read_csv(subSes)
for row in subIDs.itertuples(index=True, name='Pandas'):
    sub  = row.sub
    ses  = row.ses
    RUN  = row.RUN
    dwi  = row.dwi
    func = row.func
    source = f"/dipc/lmx/ThaTract/Nifti/derivatives/fs_7.1.1-03/analysis-01/{sub}/ses-T01/output"
    os.chdir(source)
    cmd_str = "unzip fs.zip"
    
