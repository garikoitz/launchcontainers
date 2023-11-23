import os
import pandas as pd

# Get the unique list of subjects and sessions
# basedir  = "/bcbl/home/public/Gari/MAGNO2"
basedir="/export/home/glerma/glerma/00local/PROYECTOS/MAGNO2/"
codedir  = "/bcbl/home/home_g-m/glerma/GIT/paper-MAGNO"
curatedlist=os.path.join(codedir,"curatedNiftiList.txt")
os.chdir(codedir)

# READ THE FILE
dt = pd.read_csv(curatedlist, sep="\t", header=None)
dt = dt.rename(columns={0: 'path', 1: 'sub', 2:'ses', 3:'todel',4:'torename'})
# OBTAIN UNIQUE VALUES
subses = dt.drop_duplicates(subset=["sub","ses","todel"])
subses = subses.loc[ subses['todel'] == False,:][["sub","ses"]]
# Save unique S and SS file and edit as required
subses['RUN']  = True
subses['anat'] = True
subses['dwi']  = False
subses['func'] = False
# Make only those dwi and func's true as required
subses = subses.reset_index()
subses = subses.drop('index',1)
subses = subses.drop('level_0',1)
for index in subses.index:
    sub = subses.loc[index, 'sub']
    ses = subses.loc[index,'ses']
    if os.path.isdir(os.path.join(basedir,'Nifti','sub-'+sub,'ses-'+ses,'dwi')):
        subses.loc[index,'dwi'] = True
    if os.path.isdir(os.path.join(basedir,'Nifti','sub-'+sub,'ses-'+ses,'func')):
        subses.loc[index,'func'] = True


opFile = os.path.join(codedir, 'subSesList.txt')
subses.to_csv(opFile, header=True, sep=",", index = False)


