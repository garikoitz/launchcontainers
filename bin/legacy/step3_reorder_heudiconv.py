import os
import shutil
import glob
import pandas as pd
import numpy as np
import re
import sys

SHOW = True

basedir  = "/bcbl/home/public/Gari/MAGNO2"
niftidir = os.path.join(basedir,"Nifti")
codedir  = "/bcbl/home/home_g-m/glerma/GIT/paper-MAGNO"
curatedlist=os.path.join(codedir,"curatedNiftiList.txt")
os.chdir(codedir)

# READ THE FILE
dt = pd.read_csv(curatedlist, sep="\t", header=None)
dt = dt.rename(columns={0: 'path', 1: 'sub', 2:'ses', 3:'todel',4:'torename'})
#print(dt)
# Check that everything we said it was there, it is actually there
counter = 0
for row in dt.itertuples(index=True, name='Pandas'):
    if os.path.exists(os.path.join(niftidir,row.path)):
        counter += 1
    else:
        print(os.path.join(niftidir,row.path))
print('There are '+str(len(dt))+' lines in the excel and we matched '+ str(counter) +' lines')
# if len(dt) != counter:
#     print('Fix the excel to account for the missing or extra rows')
#     sys.exit(0)






# RENAME THE REQUIRED FILES
print("Renaming required files (if any and not already done)")
dt_rename = dt.loc[dt[ (dt['torename'].notnull()) & (dt['torename']!='') ].index]
# print(dt_rename)
for row in dt_rename.itertuples(index=True, name='Pandas'):
    if os.path.isfile(os.path.join(niftidir,row.path)):
        print(os.path.join(niftidir,row.path))
        os.rename(os.path.join(niftidir,row.path), os.path.join(niftidir,row.torename))
        

# DELETE THE REQUIRED FILES
print("Deleting required files (if any and not already done)")
dt_delete = dt.loc[ dt['todel'] == True,:]
# print(dt_delete)
for row in dt_delete.itertuples(index=True, name='Pandas'):
    if os.path.isfile(os.path.join(niftidir,row.path)):
        print(os.path.join(niftidir,row.path))
        os.remove(os.path.join(niftidir,row.path))



# MOVE/RENAME THE SES-T1 files
print("Moving  required files (if any and not already done)")
dt_rename = dt.loc[ dt['todel'] == False,:]
# print(dt_rename)
for row in dt_rename.itertuples(index=True, name='Pandas'):
    if type(row.torename) == float:
        src = os.path.join(niftidir,row.path)
    else: 
        src = os.path.join(niftidir,row.torename)
    if os.path.isfile(src):
        # if SHOW: print('This src exists: '+src)
        oldsub = re.search(r'(?<=sub-)\w+', src).group(0)
        oldses = re.search(r'(?<=ses-)\w+', src).group(0)
        dst = src
        dst = dst.replace(oldsub, row.sub)
        dst = dst.replace(oldses, row.ses)

        if not os.path.isdir(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        if not os.path.isfile(dst):
            if SHOW: print('dst not, so renaming to: '+dst+'\n')
            os.rename(src, dst)
        # else:
        #     if SHOW: print('dst exist too, not renaming, dst: '+dst+'\n')

    # else:
    #     if SHOW: print('This src does not exist: ' + src)

# DELETE EMPTY FOLDERS
from remove_empty_folders import removeEmptyFolders
removeEmptyFolders(niftidir, False)
