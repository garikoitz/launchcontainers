"""
compare_nii.py
--------------
Check whether two NIfTI files are identical.

Checks:
  1. Header fields (shape, zooms, affine, data type)
  2. Voxel data (exact array equality, and if not: max absolute difference)

Usage:
  python compare_nii.py file_a.nii.gz file_b.nii.gz
"""

import sys
import numpy as np
import nibabel as nib


def compare(path_a: str, path_b: str) -> None:
    img_a = nib.load(path_a)
    img_b = nib.load(path_b)

    ok = True

    # --- shape ---
    if img_a.shape != img_b.shape:
        print(f"  DIFFER  shape:  {img_a.shape}  vs  {img_b.shape}")
        ok = False
    else:
        print(f"  OK      shape:  {img_a.shape}")

    # --- data type ---
    if img_a.get_data_dtype() != img_b.get_data_dtype():
        print(
            f"  DIFFER  dtype:  {img_a.get_data_dtype()}  vs  {img_b.get_data_dtype()}"
        )
        ok = False
    else:
        print(f"  OK      dtype:  {img_a.get_data_dtype()}")

    # --- zooms ---
    if not np.allclose(img_a.header.get_zooms(), img_b.header.get_zooms()):
        print(
            f"  DIFFER  zooms:  {img_a.header.get_zooms()}  vs  {img_b.header.get_zooms()}"
        )
        ok = False
    else:
        print(f"  OK      zooms:  {img_a.header.get_zooms()}")

    # --- affine ---
    if not np.allclose(img_a.affine, img_b.affine):
        print("  DIFFER  affine")
        print(f"    A:\n{img_a.affine}")
        print(f"    B:\n{img_b.affine}")
        ok = False
    else:
        print("  OK      affine")

    # --- voxel data ---
    data_a = img_a.get_fdata()
    data_b = img_b.get_fdata()

    if data_a.shape != data_b.shape:
        print("  DIFFER  data shape mismatch — cannot compare voxels")
        ok = False
    else:
        if np.array_equal(data_a, data_b):
            print("  OK      voxel data: exactly identical")
        else:
            diff = np.abs(data_a - data_b)
            n_differ = np.sum(diff > 0)
            print(f"  DIFFER  voxel data: {n_differ} voxels differ")
            print(f"          max |diff| = {diff.max():.6g}")
            print(f"          mean|diff| (nonzero) = {diff[diff > 0].mean():.6g}")
            ok = False

    print()
    if ok:
        print("RESULT: IDENTICAL")
    else:
        print("RESULT: DIFFERENT")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_nii.py file_a.nii.gz file_b.nii.gz")
        sys.exit(1)
    print(f"A: {sys.argv[1]}")
    print(f"B: {sys.argv[2]}")
    print()
    compare(sys.argv[1], sys.argv[2])
