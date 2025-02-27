# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024
# All rights reserved.
#
# This script is distributed under the Apache-2.0 license.
# You may use, distribute, and modify this code under the terms of the Apache-2.0 license.
# See the LICENSE file for details.
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT.
#
# Author: Yongning Lei
# Email: yl4874@nyu.edu
# GitHub: https://github.com/yongninglei
# -----------------------------------------------------------------------------
import nibabel as nib
import numpy as np
import numpy as np
import os.path as path
import shutil
import os 
from bids import BIDSLayout
import bids
import json

def convert_to_ras(input_nifti, output_nifti):
    # Load the NIfTI file
    img = nib.load(input_nifti)
    
    # Get the affine matrix and the image data
    affine = img.affine
    data = img.get_fdata()
    
    # Check the current orientation
    current_orientation = nib.orientations.aff2axcodes(affine)
    
    print(f"Current orientation: {current_orientation}")
    
    # Target orientation is RAS
    target_orientation = ('R', 'A', 'S')
    
    ornt_start=nib.orientations.axcodes2ornt(current_orientation)
    ornt_tart= nib.orientations.axcodes2ornt(target_orientation)
    # Compute the transformation to RAS
    ornt_transform = nib.orientations.ornt_transform(
        ornt_start,
        ornt_tart
    )
    
    # Apply the orientation transformation
    reoriented_data = nib.orientations.apply_orientation(data, ornt_transform)
    
    # Compute the new affine matrix
    new_affine = nib.orientations.inv_ornt_aff(ornt_transform, data.shape) @ affine
    
    # Save the reoriented image
    reoriented_img = nib.Nifti1Image(reoriented_data, new_affine, header=img.header)
    nib.save(reoriented_img, output_nifti)
    print(f"Reoriented NIfTI saved to {output_nifti}")
    return

basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
analysis_name='week1'
output_dir=path.join(basedir,'derivatives','processed_nifti',f'analysis-{analysis_name}')
layout=BIDSLayout(output_dir)

'''
There should be a function to read the note from each session, then between each fmap, the item inbetween will be used
as the intended for element

then I will need to edit the protocol name in the logs, and MRI sequences
'''

#subs = layout.get(return_type='id', target='subject')
subs= ['03','06','08'] #,'06','08'
sess= ['01']

for sub in subs:
    for ses in sess:
        niftis=layout.get(subject=sub, session=ses, extension='.nii.gz')
        for nifti in niftis:
            src_nifti= nifti.path
            src_nifti_name= src_nifti.split('/')[-1]
            backup_nifti=src_nifti.replace('.nii.gz', '_orig.nii.gz')
            targ_nifti= src_nifti
            os.chmod(src_nifti, 0o755)
            shutil.copy2(src_nifti,backup_nifti)
            convert_to_ras(backup_nifti, targ_nifti)


# this code will change the data quality of the T1, the max value is smaller
# needs to check, but not now


# new convert: to make sure the axis is correct
