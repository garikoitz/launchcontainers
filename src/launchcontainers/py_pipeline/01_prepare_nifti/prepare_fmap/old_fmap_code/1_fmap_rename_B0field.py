#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 10:51:30 2021
@author: dlinhardt
"""
import numpy as np
import os.path as path
from os import rename
import os 
from bids import BIDSLayout
import bids
import json

layout = BIDSLayout('/scratch/tlei/DATA/VOTCLOC/BIDS')

#subs = layout.get(return_type='id', target='subject')
#subs= ['01','02','03', '04','05']
subs=['05']
for sub in subs:

    #sess = layout.get(subject=sub, return_type='id', target='session')
    sess=['day3PF', 'day5BCBL', 'day6BCBL']
    print(f'working on {sub}...')

    for ses in sess:
        print(f'working on {ses}')
        # load func and fmaps
        funcNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', suffix='bold',datatype='func')
        fmapNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', datatype='fmap')

        funcNiftisMeta = [funcNiftis[i].get_metadata() for i in range(len(funcNiftis))]
        fmapNiftisMeta = [fmapNiftis[i].get_metadata() for i in range(len(fmapNiftis))]

 
        funcN = funcNiftis
        fmapN = fmapNiftis
        


        # add list to IntendedFor field in fmap json, add B0fieldIdentifier in fmap json
        for fmapNifti in fmapN:
            f = fmapNifti.path.replace('.nii.gz', '.json')
            with open(f, 'r') as file:
                j = json.load(file)
            j['B0FieldIdentifier']= f"SEFieldMap_{ses}"
            with open(f, 'w') as file:    
                json.dump(j, file, indent=2)
            print(f'rename B0FieldIdentifier for {f}')
       
        # add B0source in bold.json
        for funcNifti in funcN:
        
            f = funcNifti.path.replace('.nii.gz', '.json')
            with open(f, 'r') as file:
                j = json.load(file)

                j['B0FieldSource']= f"SEFieldMap_{ses}"
            with open(f, 'w') as file:
                json.dump(j, file, indent=2)
                print(f'rename func json for {f}')
