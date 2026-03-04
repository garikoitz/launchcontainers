"""
analysis_specs/rtp.py
======================
RTP retinotopy analysis output spec.

[DEV] Edit EXPECTED_SUFFIXES to match your pipeline outputs.
"""

from __future__ import annotations

from pathlib import Path

from .check_analysis_integrity import AnalysisSpec
from .check_analysis_integrity import default_combinations


class RTPSpec(AnalysisSpec):
    # [DEV] Change expected output file suffixes here
    EXPECTED_SUFFIXES = ["rtp_*.nii.gz"]

    @property
    def name(self) -> str:
        return "rtp"

    @property
    def description(self) -> str:
        return "RTP retinotopy analysis outputs"

    @property
    def uses_glob(self) -> bool:
        return True

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        sub = session_dir.parent.name
        ses = session_dir.name
        return {
            "rtp_outputs": [f"{sub}_{ses}_{s}" for s in self.EXPECTED_SUFFIXES],
        }

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()
