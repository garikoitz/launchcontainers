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
import os
import os.path as op
from os import makedirs

logger = logging.getLogger("Launchcontainers")


def setup_logger(quiet, verbose=False, debug=False, log_dir=None, log_filename=None):
    """
    Configure the main launchcontainers logger.

    Parameters
    ----------
    quiet : bool
        If ``True``, only critical messages are emitted to the console.
    verbose : bool, default=False
        If ``True``, emit informational messages to the console.
    debug : bool, default=False
        If ``True``, emit debug messages to the console.
    log_dir : str, optional
        Directory where ``.log`` and ``.err`` files should be written.
    log_filename : str, optional
        Basename used for the log files created under ``log_dir``.

    Returns
    -------
    logging.Logger
        Configured package logger.
    """
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    # set up the lowest level for the logger first, so that all the info will be get
    logger.setLevel(logging.DEBUG)

    # set up formatter and handler so that the logging info can go to stream or log files
    # with specific format
    log_formatter = logging.Formatter(
        "%(asctime)s (%(name)s):[%(levelname)s] \
            %(module)s - %(funcName)s() - line:%(lineno)d   $ %(message)s ",
        datefmt="%Y-%m-%d_%H:%M:%S",
    )

    stream_formatter = logging.Formatter(
        "(%(name)s):[%(levelname)s]  %(module)s:%(funcName)s:%(lineno)d %(message)s",
    )
    # Define handler and formatter
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stream_formatter)
    if verbose:
        stream_handler.setLevel(logging.INFO)
    elif quiet:
        stream_handler.setLevel(logging.CRITICAL)
    elif debug:
        stream_handler.setLevel(logging.DEBUG)
    else:
        stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)

    if log_dir:
        if not os.path.isdir(log_dir):
            makedirs(log_dir)

        file_handler_info = logging.FileHandler(
            op.join(log_dir, f"{log_filename}.log"), mode="a"
        )
        file_handler_error = logging.FileHandler(
            op.join(log_dir, f"{log_filename}.err"), mode="a"
        )
        file_handler_info.setFormatter(log_formatter)
        file_handler_error.setFormatter(log_formatter)

        file_handler_info.setLevel(logging.INFO)
        file_handler_error.setLevel(logging.ERROR)

        logger.addHandler(file_handler_info)
        logger.addHandler(file_handler_error)

    return logger


def setup_logger_create_bids(verbose=True, log_dir=None, log_filename=None):
    """
    Configure a simplified logger for the fake-BIDS helper workflow.

    Parameters
    ----------
    verbose : bool, default=True
        If ``True``, emit informational messages to the console.
    log_dir : str, optional
        Directory where helper log files should be written.
    log_filename : str, optional
        Basename used for the helper log files.

    Returns
    -------
    logging.Logger
        Configured package logger.
    """
    # set up the lowest level for the logger first, so that all the info will be get
    logger.setLevel(logging.DEBUG)

    # set up formatter and handler so that the logging info can go to stream or log files
    # with specific format
    log_formatter = logging.Formatter(
        "%(asctime)s (%(name)s):[%(levelname)s] %(module)s - "
        "%(funcName)s() - line:%(lineno)d   $ %(message)s ",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_formatter = logging.Formatter(
        "(%(name)s):[%(levelname)s]  %(module)s:%(funcName)s:%(lineno)d %(message)s",
    )
    # Define handler and formatter
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stream_formatter)
    if verbose:
        stream_handler.setLevel(logging.INFO)
    else:
        stream_handler.setLevel(logging.WARNING)
    logger.addHandler(stream_handler)

    if log_dir:
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)

        file_handler_info = logging.FileHandler(
            op.join(log_dir, f"{log_filename}_info.log"), mode="a"
        )
        file_handler_error = logging.FileHandler(
            op.join(log_dir, f"{log_filename}_error.log"), mode="a"
        )
        file_handler_info.setFormatter(log_formatter)
        file_handler_error.setFormatter(log_formatter)

        file_handler_info.setLevel(logging.INFO)
        file_handler_error.setLevel(logging.ERROR)
        logger.addHandler(file_handler_info)
        logger.addHandler(file_handler_error)

    return logger
