import os.path as op
import os
from os import makedirs
import numpy as np
from scipy import stats
import nibabel as nib
from nilearn.surface import load_surf_data
from nilearn.image import load_img
from nistats.reporting import compare_niimgs

#%%compare data from two folders

basedir='/bcbl/home//public/Gari/VOTCLOC/derivatives/fmriprep'

srcanalysis='analysis-okazaki_ST0'
targanalysis='analysis-okazaki_ST05'

subid='01'

src_filename=op.join(basedir,srcanalysis, f'sub-{subid}','anat',f'sub-{subid}_run-01_desc-preproc_T1w.nii.gz')

target_filename=op.join(basedir,targanalysis, f'sub-{subid}','anat',f'sub-{subid}_run-01_desc-preproc_T1w.nii.gz')


src_file=load_img(src_filename)
target_file=load_img(target_filename)
# compare surf,load_surf will give the data directly
#slope, intercept, r_value, p_value, std_err = stats.linregress(src_file, target_file)
# compare T1 3D, just simply reshape
slope, intercept, r_value, p_value, std_err = stats.linregress(src_file.get_fdata().reshape(224*240*256), target_file.get_fdata().reshape(224*240*256))
if r_value**2 < 0.9999:
    print(f'Attention the result r square is {r_value**2}')
print(r_value)
