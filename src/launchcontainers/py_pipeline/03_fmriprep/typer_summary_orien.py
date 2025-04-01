#!/bin/env python
# /// script
# requires-python = ">=3.13"
# dependencies = ["nibabel", "typer"]
# ///
#  This is taking from online resoouces neurostart
from __future__ import annotations

from pathlib import Path

import nibabel as nb
import typer


def main(paths: list[Path]) -> None:
    for path in paths:
        img = nb.load(path)
        axcodes = nb.aff2axcodes(img.affine)

        print(f'{path}: {axcodes}')


if __name__ == '__main__':
    typer.run(main)
