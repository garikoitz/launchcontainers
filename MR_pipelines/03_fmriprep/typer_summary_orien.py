# """
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
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
