# """
# MIT License
# Copyright (c) 2022-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
from __future__ import annotations

import os

import pandas as pd
from pathlib import Path
import typer
from launchcontainers import utils as do
from datetime import datetime
import zipfile
import json
import yaml
import subprocess as sp
"""
RTP2-pipeline 0.2.2_3.0.4
If it finished: 
    There should be 4 places that have the tracts file
    1. Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / 'tracts'
    2. Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / 'tracts.zip' (this means everything is finished)
    3. Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / "RTP_PIPELINE_ALL_OUTPUT"
    4. Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / "RTP_PIPELINE_ALL_OUTPUT.zip"

    and there wont' be 
    Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / 'RTP'

If it is not finished
    # ls sub-S069/ses-T02/output/
    config_RTP.json  log  Reproduce.mat  RTP  tmp  tracts
    There will be no Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / "RTP_PIPELINE_ALL_OUTPUT"
    And there will be Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / 'RTP'

    Inside Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output' / 'RTP' / 'mrtrix' / you can check all the tracts

"""       

# def generate_tract_path_list(analysis_dir, tract_prefix, df_subses):
#     tract = f'MNI_{tract_prefix}_clean_fa_bin.nii.gz'
#     print(tract)
#     paths = []
#     missing = []
#     for row in df_subses.itertuples(index=True):
#         sub = row.sub
#         ses = row.ses
#         RUN = row.RUN
#         if str(RUN).lower() == 'true':
#             tract_fpath = os.path.join(
#                 analysis_dir,
#                 f'sub-{sub}',
#                 f'ses-{ses}',
#                 'MNI_tract',
#                 tract,
#             )
#             # print(tract_fpath)
#             if os.path.exists(tract_fpath):
#                 paths.append(tract_fpath)
#             else:
#                 print(f'missing : sub-{sub} ses-{ses}')
#                 missing.append({'sub': sub, 'ses': ses})


def find_subseslist(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if fname.lower() == 'subseslist.txt':
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No subseslist.txt found under {analysis_dir}')

def find_lc_yaml(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if fname.lower() == 'lc_config.yaml':
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No lc_config.yaml found under {analysis_dir}')

def find_config_json(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if "rtp2-pipeline.json" in fname:
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No rtp2-pipeline.json found under {analysis_dir}')

def find_qc_csv(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if "qc_unfinished_tracts" in fname:
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No qc_unfinished_tracts.csv found under {analysis_dir}')


def check_rtp2_pipeline_logs(analysis_dir):
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    for row in df_subSes.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        if RUN == 'True' and dwi == 'True':
            log_file = os.path.join(
                analysis_dir,
                f'sub-{sub}',
                f'ses-{ses}',
                'output', 'log', 'RTP_log.txt',
            )

            if os.path.isfile(log_file):
                with open(log_file) as f:
                    lines = f.readlines()
                    # print(f'*****for sub-{sub}_ses-{ses}')
                    # print(lines[-1] + '###')
                    if lines[-1].strip() != 'Sending exit(0) signal.':
                        print(f'!!!Issue with sub-{sub}, ses-{ses}*****\n')
            else:
                print(f'Log file missing for sub-{sub}, ses-{ses}')

def get_finished_tract(subses_dir):
    '''
    Function to get the tract that are finished
        will look in the subses/output/dir
    return a list
    '''
    tracts_zip = subses_dir /'output' / 'tracts.zip'
    RTP_PIPELINE_ALL_OUTPUT_zip = subses_dir /'output' / 'RTP_PIPELINE_ALL_OUTPUT.zip'
    RTP_mrtrix_dir = subses_dir /'output' / 'RTP' / 'mrtrix'
    if tracts_zip.exists() or RTP_PIPELINE_ALL_OUTPUT_zip.exists() and RTP_mrtrix_dir.exists():
        zip_path = tracts_zip if tracts_zip.exists() else RTP_PIPELINE_ALL_OUTPUT_zip
        # 1. use tracts.zip
        with zipfile.ZipFile(zip_path,"r") as z:
            names = z.namelist()
            only_prefix=["_".join(i.strip("tracts/").split('_')[0:3]).strip(".tck") for i in names if i.startswith('tracts') ]
            tract_prefix_set=set(only_prefix)
            tract_prefix_set.discard('')
        
        counts = []
        for tract_prefix in tract_prefix_set:
            n = sum(x.startswith(tract_prefix) for x in only_prefix)
            counts.append({"tract": tract_prefix, "count": n})
        # Create dataframe
        df1 = pd.DataFrame(counts)
        # if it's already in zip, 9 files per tract
        df1['finished']=df1['count']==9
        df=df1[df1['finished']]['tract'].to_list()
    elif ( not tracts_zip.exists() and not RTP_PIPELINE_ALL_OUTPUT_zip.exists()) and RTP_mrtrix_dir.exists():
    # 3 use the mrtrix folder under the RTP/ tracts
        names4 = os.listdir(RTP_mrtrix_dir)
        only_prefix4=["_".join(i.split('_')[0:3]).strip(".tck") for i in names4 if 'tmp' not in i and 'dwi' not in i]
        # use set to get the unique prefix and drop ''
        tract_prefix_set4=set(only_prefix4)
        tract_prefix_set4.discard('')
        
        counts = []
        for tract_prefix in tract_prefix_set4:
            n = sum(x.startswith(tract_prefix) for x in only_prefix4)
            counts.append({"tract": tract_prefix, "count": n})
        # Create dataframe
        df1 = pd.DataFrame(counts)
        # if its from the RTP / mrtrix
        # each tract need 12 files to be there
        df1['finished']=df1['count']==12
        df=df1[df1['finished']]['tract'].to_list()
    else:
        print("There are nothing finished for")
        df=[]
    # # 3. use the RTP_PIPELINE_ALL_OUTPUT folder
    # names3 = os.listdir(RTP_PIPELINE_ALL_OUTPUT_dir)
    # only_prefix3=["_".join(i.split('_')[0:3]).strip(".tck") for i in names3]
    # # use set to get the unique prefix and drop ''
    # tract_prefix_set3=set(only_prefix3)
    # tract_prefix_set3.discard('')
    
    # counts = []
    # for tract_prefix in tract_prefix_set3:
    #     n = sum(x.startswith(tract_prefix) for x in only_prefix3)
    #     counts.append({"tract": tract_prefix, "count": n})
    # # Create dataframe
    # df3 = pd.DataFrame(counts)
    # df3['finished']=df3['count']==9

    #### for the old RTP, we shouldn't use this as ref because it is old
    #### for the new RTP, we have to use this
    return df

def qc_output_tract_all_finished(analysis_dir):
    '''
    This function is used to read the analysis dir, get subseslist, config yaml

    and then find the tractparams that being used

    then it will check if output is having everything as input tractparams

    it will store the missing tractparams per sub/ses so that we will create rerun sessions
    
    Input: analysis dir

    Output: QC dataframe (in csv) stores the sub ses missing-tract
    '''
    # from the lc_config.yaml or the config.json or under the sub/ses/input dir
    # get the real tractparams.tsv that has been send to the analysis
    analysis_dir = Path(analysis_dir)
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    subses_tracts=[]
    for row in df_subSes.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        if RUN == 'True' and dwi == 'True':
            subses_dir = analysis_dir / f'sub-{sub}' / f'ses-{ses}'
            path_tractparams = subses_dir / 'input' /'tractparams'/ 'tractparams.csv'
            tractparam_df, _ = do.read_df(path_tractparams)
            intended_tract_prefix = set([i.replace('-',"_") for i in tractparam_df["slabel"].to_list()])
            finished_trats_prefix = get_finished_tract(subses_dir)
            # get the one need rerun
            need_rerun= [x for x in intended_tract_prefix if x not in finished_trats_prefix]
            # remove the one contains Sup Ang IPS0 IPS1
            need_rerun = [x for x in need_rerun if 'Sup' not in x and 'Ang' not in x and 'IPS' not in x]
            if len(need_rerun) !=0:
                print(f"We need to rerun some tracts for {sub}-{ses}")
                for tract in need_rerun:
                    rerun_subses={
                        "sub":sub,
                        "ses":ses,
                        "tracts":tract,
                        "shemi":tract.split('_')[0],
                        "nhlabel":'-'.join(tract.split('_')[1:3])
                    }
                    subses_tracts.append(rerun_subses)
    
    rerun_df=pd.DataFrame(subses_tracts)
    if len(rerun_df)!=0:
        now_str= datetime.now().strftime("%Y-%m-%d-%H:%M")
        # currently it is hard coded and there is no time stamp
        qc_output= analysis_dir / f"qc_unfinished_tracts.csv"
        rerun_df.to_csv(qc_output, index=False)
    else:
        print("Congrats, RTP2-pipeline finished successfully!")
        rerun_df = []
    return rerun_df

def create_rerun_configs(analysis_dir):
    '''
    From the QC dataframe generate a batch lc_config.yaml, subseslist(with only one sub) and tractparams(with several tracts)
    Launch a bunch of one ses analysis to rerun everything

    '''
    analysis_dir = Path(analysis_dir)
    # get the inputs from analysis dir
    output_dir_all_configs = analysis_dir.parent / f'rerun_configs_{analysis_dir.name}'
    #create the directory 
    os.makedirs(output_dir_all_configs, exist_ok=True)

    qc_df_path=  find_qc_csv(analysis_dir)
    # read the qc_df, will only get the prefix qc_unfinished_tracts and ignore the time
    qc_df = pd.read_csv(qc_df_path)
    # for all the sub in the qc_unfinished_tracts, 
    # get all the missing tracts and generate a tractparams file
    subs = qc_df['sub'].unique().tolist()
    sub_group = qc_df.groupby('sub')
    for sub in subs:
        sub_rerundf=sub_group.get_group(sub)
        # todo implement multisession rerun here
        ses_group = sub_rerundf.groupby('ses')
        sess = sub_rerundf['ses'].unique().tolist()
        for ses in sess:
            subses_rerundf = ses_group.get_group(ses)
            # edit lc_config.yaml
            # edit tractparams
            prepare_RTP2_pipeline_subses_rerun(analysis_dir, output_dir_all_configs, subses_rerundf)

    # edit rtp2-pipeline.json
    # take from analysis dir and remove the input field 
    config_json_path =  find_config_json(analysis_dir)
    with open (config_json_path, "r") as j:
        data=json.load(j)
    # Remove the inputs field
    data.pop("inputs",None)
    new_config_json = output_dir_all_configs / Path(config_json_path).name
    # write to the new json, we will use this one for all
    with open(new_config_json,'w') as f:
        json.dump(data,f,indent=4)
    print(f"rtp2pipeline.json created new basic template")
    

def prepare_RTP2_pipeline_subses_rerun(analysis_dir, output_dir_all_configs, subses_rerundf):

    # read lc_config yaml
    lc_config_path = find_lc_yaml(analysis_dir)
    lc_config = do.read_yaml(lc_config_path)

    # generate the new analysis name 
    sub = subses_rerundf['sub'].unique()[0]
    ses = subses_rerundf['ses'].unique()[0]
    num_of_tracts=len(subses_rerundf)
    new_analysis_name = f"rerun_sub-{sub}_ses-{ses}_{num_of_tracts}-tracts"
    # edit the lc_config params for the new analysis
    lc_config['general']['analysis_name'] = new_analysis_name
    container = lc_config['general']['container']
    # generate the tractparams name
    new_tractparams_name = f'tractparams_{new_analysis_name}.csv'
    output_tractparams_path = output_dir_all_configs / new_tractparams_name
    lc_config['container_specific'][container]['tractparams']= str(output_tractparams_path)
    new_lc_config_path = output_dir_all_configs / f'lc_config_{new_analysis_name}.yaml'
    # save the new lc_config yaml
    with open(new_lc_config_path,'w') as f:
        yaml.dump(lc_config, f, default_flow_style= False, sort_keys=False)
    print(f"new_lc_config_yaml created for {sub}-{ses}")
    # prepare the subses
    subseslist_name = f'subseslist_{new_analysis_name}.txt'
    df_subses=pd.DataFrame(
        {
            'sub':[sub],
            'ses':[ses],
            'RUN':["True"],
            'anat':["True"],
            'dwi':["True"],
            'func':["True"]
        }
    )
    output_subses_path = output_dir_all_configs / subseslist_name
    df_subses.to_csv(output_subses_path, index=False, sep=',')
    print(f"new_subseslist created for {sub}-{ses}")
    # prepare the customize tractparams
    # get the subses input tractparams
    subses_dir = analysis_dir / f'sub-{sub}' / f'ses-{ses}'
    path_tractparams = subses_dir / 'input' /'tractparams'/ 'tractparams.csv'
    tractparam_df, _ = do.read_df(path_tractparams)
    # take the tract that needs rerun:
    keep = tractparam_df.merge(
        subses_rerundf[['shemi','nhlabel']].drop_duplicates(),
        on=['shemi','nhlabel'],
        how='inner'
    )

    keep.to_csv(output_tractparams_path, index=False)
    print(f"new_tract_params created for {sub}-{ses}")
    return

def gen_batch_lc_script(analysis_dir, step):
    '''
    After prepare all the jsons, it's time to do a launch prepare and then a launch run_lc

    this function will read from the analysis-paper_dv-main/qc_unfinished_tracts

    and it will generate a bunch of commands that can be used to launch lc prepare mode

    Step: prepare or run
    '''
    analysis_dir = Path(analysis_dir)
    # get the inputs from analysis dir
    output_dir_all_configs = analysis_dir.parent / f'rerun_configs_{analysis_dir.name}'
    # get the qc_unfinished df
    qc_df_path=  find_qc_csv(analysis_dir)
    # read the qc_df, will only get the prefix qc_unfinished_tracts and ignore the time
    qc_df = pd.read_csv(qc_df_path)
    # for all the sub in the qc_unfinished_tracts, 
    subs = qc_df['sub'].unique().tolist()
    sub_group = qc_df.groupby('sub')
    cmds = []
    for sub in subs:
        sub_rerundf=sub_group.get_group(sub)
        # todo implement multisession rerun here
        ses_group = sub_rerundf.groupby('ses')
        sess = sub_rerundf['ses'].unique().tolist()
        for ses in sess:
            subses_rerundf = ses_group.get_group(ses)
            # then according to sub ses and number of tracts, we are able to get the analysis name
            sub = subses_rerundf['sub'].unique()[0]
            ses = subses_rerundf['ses'].unique()[0]
            num_of_tracts=len(subses_rerundf)
            new_analysis_name = f"rerun_sub-{sub}_ses-{ses}_{num_of_tracts}-tracts"
            if step == 'prepare':
                cmd = gen_single_lc_prepare_script(output_dir_all_configs,new_analysis_name)
                cmds.append(cmd)
            elif step == 'run':
                cmd = gen_single_lc_run_script(analysis_dir, new_analysis_name)
                cmds.append(cmd)                    
    num_of_prepare = len(cmds)
    batch_command_file = output_dir_all_configs / f"batch_lc_{step}_command.txt"
    # store the list of commands in a command.txt file
    with open(batch_command_file, 'w') as f:
        for cmd in cmds:
            f.write(f'{cmd}\n')
    return cmds

def gen_single_lc_prepare_script(output_dir_all_configs,new_analysis_name):
    # make sure the work dir is a path object
    work_dir = Path(output_dir_all_configs)
    lc_cofig_name = f'lc_config_{new_analysis_name}.yaml'
    subseslist_name = f'subseslist_{new_analysis_name}.txt'
    config_json_name = f'rtp2-pipeline.json'
    lc_config_path = work_dir / lc_cofig_name
    subseslist_path = work_dir / subseslist_name
    config_json_path = work_dir / config_json_name
    cmd = (
        f'lc prepare '
        f'-lcc {lc_config_path} '
        f'-ssl {subseslist_path} '
        f'-cc {config_json_path} '
    )
    return cmd


def gen_single_lc_run_script(analysis_dir, new_analysis_name):
    # make sure the work dir is a path object
    work_dir = analysis_dir.parent / f'analysis-{new_analysis_name}'
    cmd = (
        f'lc run '
        f'-w {work_dir} '
        f'--run_lc '
    )
    return cmd

def launch_cmd(cmds):
    for cmd in cmds:
        print(cmd)
        sp.run(cmd, shell=True)

# finally do a rsync and get all the result
if __name__ == '__main__':
    analysis_dir = Path('/bcbl/home/public/DB/devtrajtract/DATA/MINI/nifti/derivatives/rtp2-pipeline_0.2.1_3.0.4rc2/analysis-paper_dv-main')
    # TODO: check, if the qc_df is already there and the command.txt or the jsons are already there
    # no need to rerun everything

    # first QC the analysis dir to see if we need rerun some subses
    rerun_df=qc_output_tract_all_finished(analysis_dir)
    # if rerun_df is not None:
    if len(rerun_df) > 0:
        # create the config files for each sub's rerun
        create_rerun_configs(analysis_dir)
        # generate lc prepare script 
        cmds= gen_batch_lc_script(analysis_dir,'run')
        launch_cmd(cmds)

