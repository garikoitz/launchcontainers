# clusters/local.py
from __future__ import annotations

import subprocess
from multiprocessing import Pool


def _run_cmd(cmd: str):
    return subprocess.run(cmd, shell=True).returncode


def run_local(cmds: list[str], parallel: bool = False, n_procs: int = 4):
    """
    Run commands in-process, either serially or with a process-pool.
    """
    if parallel:
        with Pool(processes=n_procs) as p:
            return p.map(_run_cmd, cmds)
    else:
        return [_run_cmd(cmd) for cmd in cmds]
