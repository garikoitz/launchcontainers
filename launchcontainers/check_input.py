#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 14:20:19 2023

@author: leiyongning
"""
#%%
import createsymlinks as csl
import pandas as pd
#%%
"""
Some documentation here

"""


#%%
check_input(configFile, subSesList)


def check_rtppreproc_input(config, sub, ses):
    """
    take the config file info to search the file name
    
    sub and ses from main() 
    
    Returns
    -------
    Nono

    """
    return 



def check_input_file_is_there (dict_config, df_subSes):
    """
    the two inputs: config is a dict, 
    
    subSesList is a dataframe,
    """
    container = dict_config["config"]["container"]
    
    for row in df_subSesList.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
        func = row.func
        if RUN and ("rtppreproc" in container):
            check_rtppreproc_input(dict_config, sub, ses)
        #if RUN and ("anatrois" in container):
            # check_anatrois(dict_config, sub, ses)

    return True or False, container 



def prepare_input_file():
    if check_file_is_there() == True :
        print ("\n the input file is there, if you want to overwrite, change force to ture in config.yaml")
    else:
        if "rtppreproc" in container:
            csl.rtppreproc(config, sub, ses)
        
    
    
    return 

if __name__ == "__main__":
    main()