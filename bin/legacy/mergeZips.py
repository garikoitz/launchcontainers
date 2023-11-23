# I am usuing it pasted in jupyter

import zipfile as z
import os
# import glob as glob

os.chdir('/Users/glerma/soft/anatROIs/templates')

zips = ['example_MORI_ROIs.zip', 'MORIROI_withCC.zip', 'MNI_ROIS_FOR_SLF.zip']

"""
Open the first zip file as append and then read all
subsequent zip files and append to the first one
"""
with z.ZipFile(zips[0], 'a') as z1:
    for fname in zips[1:]:
        zf = z.ZipFile(fname, 'r')
        for n in zf.namelist():
            z1.writestr(n, zf.open(n).read())
