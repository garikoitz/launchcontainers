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

import os
import os.path as op
import shutil
import sys
import errno
from datetime import datetime

import pandas as pd
import yaml
from yaml.loader import SafeLoader

from launchcontainers.log_setup import console


def parse_hms(ts: str) -> str:
    """
    Normalise any time string to zero-padded HH:MM:SS.
    Handles ISO datetime, sub-seconds, single-digit hours.
    """
    s = str(ts).strip()
    if "T" in s:
        s = s.split("T")[1]
    s = s.split(".")[0]
    for fmt in ("%H:%M:%S", "%H:%M", "%H%M%S", "%H%M"):
        try:
            return datetime.strptime(s, fmt).strftime("%H:%M:%S")
        except ValueError:
            continue
    return s


def times_match(t1: str, t2: str, max_diff_sec: int = 30) -> bool:
    """Return True if \|t1 - t2\| <= max_diff_sec."""
    if t1 is None or t2 is None:
        return False
    try:
        dt1 = datetime.strptime(parse_hms(t1), "%H:%M:%S")
        dt2 = datetime.strptime(parse_hms(t2), "%H:%M:%S")
    except ValueError:
        return False
    return abs((dt1 - dt2).total_seconds()) <= max_diff_sec


def die(*args):
    """
    Log an error message and terminate the current process.

    Parameters
    ----------
    *args
        Positional message parts forwarded to :meth:`console.print`.
    """
    console.print(*args, style="red")
    sys.exit(1)


def read_yaml(path_to_config_file):
    """
    Read a YAML configuration file.

    Parameters
    ----------
    path_to_config_file : str or path-like
        Path to the YAML file to load.

    Returns
    -------
    dict
        Parsed YAML contents.
    """
    with open(path_to_config_file) as v:
        config = yaml.load(v, Loader=SafeLoader)

    return config


def read_df(path_to_df_file):
    """
    Read a CSV file into a dataframe and count runnable rows.

    This helper is primarily used for ``subseslist`` files, where the
    ``RUN`` column marks which rows should be scheduled.

    Parameters
    ----------
    path_to_df_file : str or path-like
        Path to the CSV file to read.

    Returns
    -------
    tuple[pandas.DataFrame, int | None]
        The loaded dataframe and the number of rows where ``RUN == 'True'``.
        If the file does not contain a ``RUN`` column, the count is ``None``.
    """
    df = pd.read_csv(path_to_df_file, sep=",", dtype=str)
    try:
        num_of_true_run = len(df.loc[df["RUN"] == "True"])
    except Exception:
        num_of_true_run = None
        # logger.warn(f'The df you are reading is not subseslist'
        #    +f'or something is wrong {e}')
    console.print(df.head(5), style="cyan")

    return df, num_of_true_run


def copy_file(src_file, dst_file, force):
    """
    Copy a file to a destination path with optional overwrite behavior.

    Parameters
    ----------
    src_file : str or path-like
        Source file to copy.
    dst_file : str or path-like
        Destination file path.
    force : bool
        If ``True``, overwrite an existing destination file.

    Returns
    -------
    str
        Destination path.

    Raises
    ------
    FileExistsError
        If the source file does not exist.
    """
    console.print("\n" + "=" * 30 + "COPY_FILE_WORKING" + "=" * 30, style="cyan")
    if not os.path.isfile(src_file):
        console.print("\n \u274c Source file does not exist.", style="red")
        raise FileExistsError("the source file is not here")

    try:
        if ((not os.path.isfile(dst_file)) or (force)) or (
            os.path.isfile(dst_file) and force
        ):
            shutil.copy(src_file, dst_file)
            console.print(
                "\n"
                + f"\u2705 {src_file} has been successfully copied to \
                     {os.path.dirname(dst_file)} directory \n"
                + "\U0001f37a REMEMBER TO CHECK/EDIT TO HAVE THE CORRECT PARAMETERS IN THE FILE\n",
                style="cyan",
            )
        elif os.path.isfile(dst_file) and not force:
            console.print(
                "\n" + f"\u274c copy are not operating, the {src_file} already exist",
                style="yellow",
            )

    # If source and destination are the same
    except shutil.SameFileError:
        console.print(
            "\n \u274c Source and destination represent the same file.\n", style="red"
        )

    # If there is any permission issue, skip it
    except PermissionError:
        console.print(
            f"\n \u274c Permission denied: {dst_file}. Skipping...\n", style="yellow"
        )

    # For other errors
    except Exception as e:
        console.print(
            f"\n \u274c Error occurred while copying file: {e}\n", style="red"
        )

    return dst_file


def copy_configs(output_path, force=True):
    """
    Copy packaged example configuration files into a target directory.

    Parameters
    ----------
    output_path : str or path-like
        Directory where the example configuration files should be copied.
    force : bool, default=True
        If ``True``, overwrite existing files in ``output_path``.
    """
    # first, know where the tar file is stored
    import pkg_resources

    config_path = pkg_resources.resource_filename("launchcontainers", "example_configs")

    # second, copy all the files from the source folder to the output_path
    all_cofig_files = os.listdir(config_path)
    for src_fname in all_cofig_files:
        src_file_fullpath = op.join(config_path, src_fname)
        targ_file_fullpath = op.join(output_path, src_fname)
        copy_file(src_file_fullpath, targ_file_fullpath, force)

    return


def force_symlink(file1, file2, force):
    """
    Create or refresh a symbolic link and validate the result.

    Parameters
    ----------
    file1 : str or path-like
        Link target, usually an output from a previous processing step.
    file2 : str or path-like
        Link path to create for the current container input.
    force : bool
        If ``True``, replace an existing link at ``file2``.

    Raises
    ------
    OSError
        If the source is missing or the link cannot be created.
    """
    console.print("\n" + "=" * 30 + "SYMLINK_WORKING" + "=" * 30, style="cyan")
    # If force is set to False (we do not want to overwrite)
    if not force:
        try:
            # Try the command, if the files are correct and the symlink does not exist, create one

            os.symlink(file1, file2)
            console.print(
                "\n"
                + f"\u2705 Created symlink for source file: {file1} and destination file: {file2}\n",
                style="cyan",
            )
        # If raise [erron 2]: file does not exist, print the error and pass
        except OSError as n:
            if n.errno == 2:
                console.print(
                    "\n" + "Input files are missing, please check \n", style="red"
                )
                pass
            # If raise [errno 17] the symlink exist,
            # we don't force and print that we keep the original one
            elif n.errno == errno.EEXIST:
                console.print(
                    "\n" + " Symlink exist, not overwriting, remain old \n",
                    style="yellow",
                )
            else:
                console.print("\n" + "Unknown error, break the program", style="red")
                raise n

    # If we set force to True (we want to overwrite)
    if force:
        console.print(
            "\n"
            + "---force is set to True, we will overwrite the existing symlink if it exist\n",
            style="cyan",
        )
        try:
            # Try the command, if the file are correct and symlink not exist, it will create one
            os.symlink(file1, file2)
            console.print(
                "\n"
                + f"\u2705 Created symlink for source file: {file1} and destination file: {file2}\n",
                style="cyan",
            )
        # If the symlink exists, OSError will be raised
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(file2)
                console.print("\n" + "Overwriting the existing symlink", style="yellow")
                os.symlink(file1, file2)
                console.print(
                    "\n"
                    + f"\u2705 Created symlink for source file: {file1} and destination file: {file2}\n",
                    style="cyan",
                )
            elif e.errno == 2:
                console.print(
                    "\n"
                    + "\u274c Input files are missing, please check that they exist\n",
                    style="red",
                )
                raise e
            else:
                console.print(
                    "\n" + "\u274c ERROR\n" + "We do not know what happened\n",
                    style="red",
                )
                raise e
    check_symlink(file2)
    return


def check_symlink(path: str) -> None:
    """
    Validate that a path is an existing symbolic link target.

    Parameters
    ----------
    path : str
        Link path to inspect.

    Raises
    ------
    FileNotFoundError
        If ``path`` is a symlink but its target does not exist.
    """
    if op.islink(path):
        if op.exists(path):
            console.print(
                f" √ Symlink {path!r} is valid and points to {op.realpath(path)!r}",
                style="cyan",
            )
        else:
            target = os.readlink(path)
            console.print(
                f"X Symlink {path!r} is broken (target {target!r} not found)",
                style="red",
            )
            raise FileNotFoundError(f"Broken symlink: {path!r} → {target!r}")

    else:
        console.print(f" {path!r} is not a symlink", style="cyan")
