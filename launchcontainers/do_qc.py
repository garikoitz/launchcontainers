# """
# MIT License
# Copyright (c) 2020-2025 Garikoitz Lerma-Usabiaga
# Copyright (c) 2020-2022 Mengxing Liu
# Copyright (c) 2022-2023 Leandro Lecca
# Copyright (c) 2022-2025 Yongning Lei
# Copyright (c) 2023 David Linhardt
# Copyright (c) 2023 Iñigo Tellaetxe
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
from __future__ import annotations

import logging
import os.path as op
from datetime import datetime

from launchcontainers import config_logger
from launchcontainers import utils as do

logger = logging.getLogger('QC')


def qc():
    return


def main(parse_namespace):
    # 1. setup run mode logger
    # read the yaml to get input info
    analysis_dir = parse_namespace.workdir
    logging_dir = parse_namespace.log_dir
    print(f' The logging dir is {logging_dir}')
    # get the dir and fpath for launchcontainer logger
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    logging_fname = f'qc_log_{timestamp}'
    # set up the logger for prepare mode
    debug = parse_namespace.debug
    logger = config_logger.setup_logger(False, True, debug, logging_dir, logging_fname)

    # read LC config yml from analysis dir
    lc_config_fpath = op.join(analysis_dir, 'lc_config.yaml')
    lc_config = do.read_yaml(lc_config_fpath)
    logger.info('\n do_qc.main() reading lc config yaml')

    # Get general information from the config.yaml file
    # bidsdir_name = lc_config['general']['bidsdir_name']
    container = lc_config['general']['container']
    # if container is freesurgerator or anatrois, there is nothing to check
    if container in ['freesurferator', 'anatrois']:
        logger.info('\n There is not yet any QC for freesurferator or anatrois, finishing')

    # if it is rtppreproc, or rtp2-preproc, check if the output log has the field success,
    if container in ['freesurferator', 'anatrois']:
        logger.info('\n There is not yet any QC for freesurferator or anatrois, finishing')
    #
    #
    # if not,
    # generate a new subseslist based on the already have subseslist,
    # mark the ones that are not success
    # create new folder QC_{timestamp}_failed
    # keep running it, until you observed a QC_success

    # if it is rtp2-pipeline or rtppipeline, check if the output log has the field success, if not,
    # check which tract are not finished, generate new tractparams and generate new subseslist
    # create new folder QC_{timestamp}_failed
    # keep running it, until observed a QC_success

    return
