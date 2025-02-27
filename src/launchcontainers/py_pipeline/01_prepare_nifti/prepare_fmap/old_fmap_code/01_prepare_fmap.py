#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 10:51:30 2021
@author: dlinhardt
"""
import numpy as np
import os.path as path
import shutil
import os 
from bids import BIDSLayout
import bids
import json

'''
For this step, it is more effective to pass a list of sub and ses,

because reading the layout takes long time

'''

def populate_intended_for(layout_fmap, layout_func, sub, ses, basedir, output_dir, force):
    print(f'working on {sub} and {ses}...')
    # load func and fmaps
    bold_niftis = layout_func.get(subject=sub, session=ses, extension='.nii.gz', suffix='bold',datatype='func')
    sbref_niftis = layout_func.get(subject=sub, session=ses, extension='.nii.gz', suffix='sbref',datatype='func')
    
    func_nifties = bold_niftis+sbref_niftis
    fmap_niftis = layout_fmap.get(subject=sub, session=ses, extension='.nii.gz', datatype='fmap')

    # get the meta data from functional niftis and fmap nifties
    func_niftis_meta = [func_nifties[i].get_metadata() for i in range(len(func_nifties))]
    fmap_niftis_meta = [fmap_niftis[i].get_metadata() for i in range(len(fmap_niftis))]

    funcN = func_nifties
    # fmapN = np.array(fmapNiftis)[[i['RepetitionTime'] == res for i in fmapNiftisMeta]]
    fmapN = fmap_niftis
    
    # make list with all relative paths of func
    funcNiftisRelPaths = [path.join(*funcN[i].relpath.split("/")[1:]) for i in range(len(funcN))]
    funcNiftisRelPaths = [fp for fp in funcNiftisRelPaths if ((fp.endswith('_bold.nii.gz') or 
                                                                fp.endswith('_sbref.nii.gz')) and 
                                                                all([k not in fp for k in ['magnitude', 'phase']]))]
    output_fmap_dir=path.join(output_dir, f'sub-{sub}', f'ses-{ses}', 'fmap')
    if not os.path.isdir(output_fmap_dir):
        os.makedirs(output_fmap_dir)
    
    # add list to IntendedFor field in fmap json
    for fmapNifti in fmapN:
        src_fmap_json= fmapNifti.path.replace('.nii.gz', '.json')
        src_fmap_json_name= src_fmap_json.split('/')[-1]

        targ_fmap= fmapNifti.path.replace(basedir, output_dir)
        targ_fmap_json=fmapNifti.path.replace(basedir, output_dir).replace('.nii.gz', '.json')
        backup_fmap_json=src_fmap_json.replace('.json', '_orig.json')
        
        if not path.exists(targ_fmap):

            shutil.copy2(fmapNifti.path, targ_fmap)
            print(f'Copy the fmap nifti to {output_fmap_dir}')
        elif path.exists(targ_fmap) and force:
            print('overwrite the targ fmap')
            shutil.copy2(fmapNifti.path, targ_fmap)
        else:
             print(f'fmap nii is there in {output_fmap_dir}')            
        if not path.exists(backup_fmap_json):
            print(f'{src_fmap_json_name} has not been processed')
            
            with open(src_fmap_json, 'r') as file:
                j = json.load(file)
            if not 'IntendedFor' in j: #or not 'B0FieldIdentifier' in j:
                if not 'IntendedFor' in j:
                    print(f'add intendedfor for {src_fmap_json_name}')
                    j['IntendedFor'] = [f.replace("\\", "/") for f in funcNiftisRelPaths]
                else:
                    print('IntendedFor is there, do nothing')
                # if not 'B0FieldIdentifier' in j:
                #     print(f'adding B0FieldIdentifier for {backup_fmap_json}')
                #     j['B0FieldIdentifier']= "SEFieldMap"
                # else:
                #     print('B0FieldIdentifier is already there')
                shutil.copy2(src_fmap_json, backup_fmap_json)
                print(f'Creating orig.json for {src_fmap_json}')
                
                with open(targ_fmap_json, 'w') as file:
                    json.dump(j, file, indent=2)
                print(f'save json for {targ_fmap_json}')
            else:
                print('IntendedFor is already there') #AND B0FieldIdentifier
        elif path.exists(backup_fmap_json) and force:
            print(f'{backup_fmap_json} is been used to create the output fmap json')
            os.remove(targ_fmap_json)
            with open(src_fmap_json, 'r') as file:
                j = json.load(file)
            if not 'IntendedFor' in j: #or not 'B0FieldIdentifier' in j:
                if not 'IntendedFor' in j:
                    print(f'add intendedfor for {src_fmap_json_name}')
                    j['IntendedFor'] = [f.replace("\\", "/") for f in funcNiftisRelPaths]
                else:
                    print('IntendedFor is there, do nothing')
                # if not 'B0FieldIdentifier' in j:
                #     print(f'adding B0FieldIdentifier for {backup_fmap_json}')
                #     j['B0FieldIdentifier']= "SEFieldMap"
                # else:
                #     print('B0FieldIdentifier is already there')
                shutil.copy2(src_fmap_json, backup_fmap_json)
                print(f'Creating orig.json for {src_fmap_json}')
                
                with open(targ_fmap_json, 'w') as file:
                    json.dump(j, file, indent=2)
                print(f'save json for {targ_fmap_json}')
            else:
                print('IntendedFor is already there') #AND B0FieldIdentifier            

        else:
            print(f'{backup_fmap_json} has EXISTS!')

    # # add B0source in bold.json
    # for funcNifti in funcN:
    #     bold_json= funcNifti.path.replace('.nii.gz', '.json')
    #     bold_json_name= bold_json.split('/')[-1]
    #     backup_bold_json=bold_json.replace('.json', '_orig.json')
    #     if not path.exists(backup_bold_json):
    #         print(f'{bold_json_name} No backup')


    #         with open(bold_json, 'r') as file:
    #             j = json.load(file)

    #         if not 'B0FieldSource' in j:
    #             print('Adding B0FieldSource to func json')
    #             j['B0FieldSource']= "SEFieldMap"
                
    #             shutil.copy2(bold_json, backup_bold_json)
    #             print(f'Creating orig.json for {bold_json}')
    #             with open(bold_json, 'w') as file:
    #                 json.dump(j, file, indent=2)
    #             print(f'save json for {bold_json}')
    #         else:
    #             print('B0FieldSource already in func json, do nothing')
            

    #     else:
    #         print(f'{backup_bold_json} EXISTS')

    #         with open(bold_json, 'r') as file:
    #             j = json.load(file)

    #         if not 'B0FieldSource' in j:
    #             print('Adding B0FieldSource to func json')
    #             j['B0FieldSource']= "SEFieldMap"
    #         else:
    #             print('B0FieldSource already in func json, do nothing')
    #         with open(bold_json, 'w') as file:
    #             json.dump(j, file, indent=2)


def main():
    basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti_wrongscantsv'
    layout_fmap = BIDSLayout(basedir)

    output_dir='/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
    layout_func=BIDSLayout(output_dir)
    force = True
    '''
    There should be a function to read the note from each session, then between each fmap, the item inbetween will be used
    as the intended for element

    then I will need to edit the protocol name in the logs, and MRI sequences
    '''

    #subs = layout.get(return_type='id', target='subject')
    subs= ['06'] #,'06','08'
    sess= ['01','02','03']

    for sub in subs:
        for ses in sess:
            populate_intended_for(layout_fmap, layout_func, sub, ses, basedir, output_dir, force)

if __name__ == "__main__":
    main()

        