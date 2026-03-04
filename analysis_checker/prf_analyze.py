"""
analysis_specs/prf_analyze.py
==============================
PRF analyze output spec.

Expected structure:
    analysis_dir / sub-XX / ses-XX / {run_prefix}_{hemi}_{suffix}

Groups: one per run prefix (discovered from files on disk).
Each group = prefix × hemi × suffix.
"""

from __future__ import annotations

from pathlib import Path

from .check_analysis_integrity import AnalysisSpec
from .check_analysis_integrity import default_combinations


class PRFAnalyzeSpec(AnalysisSpec):
    """
    PRF analyze results: analysis_dir / sub-XX / ses-XX / <files>

    Files grouped by run prefix (discovered from files on disk).
    Each run × hemi expects a fixed set of output suffixes.

    ─── To change expected outputs ──────────────────────────────────────────
    [DEV] Edit HEMIS or EXPECTED_SUFFIXES below.
          No changes needed in the engine or registry.
    """

    HEMIS = ["hemi-L", "hemi-R"]

    EXPECTED_SUFFIXES = [
        "centerx0.nii.gz",
        "centery0.nii.gz",
        "estimates.json",
        "modelpred.nii.gz",
        "r2.nii.gz",
        "results.mat",
        "sigmamajor.nii.gz",
        "sigmaminor.nii.gz",
        "testdata.nii.gz",
        "theta.nii.gz",
    ]

    @property
    def name(self) -> str:
        return "prfanalyze"

    @property
    def description(self) -> str:
        return (
            f"PRF analyze outputs — {len(self.EXPECTED_SUFFIXES)} files "
            f"× {len(self.HEMIS)} hemis per run"
        )

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        """Discover run prefixes from files on disk, build expected file list."""
        if not session_dir.is_dir():
            return {}

        prefixes: set[str] = set()
        for f in session_dir.iterdir():
            if f.is_file() and "_hemi-" in f.name:
                prefixes.add(f.name.split("_hemi-")[0])

        return {
            prefix: [
                f"{prefix}_{hemi}_{suffix}"
                for hemi in self.HEMIS
                for suffix in self.EXPECTED_SUFFIXES
            ]
            for prefix in sorted(prefixes)
        }

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()
