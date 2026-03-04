"""
analysis_specs/prf_prepare.py
==============================
PRF prepare output spec.

Expected structure:
    analysis_dir / sub-XX / ses-XX / func / <files>

Three file groups per session:
  maskinfo         — fixed list of per-hemi ROI mask JSONs (L and R differ)
  events-{task}    — per-task per-run event TSVs (discovered)
  bold-{task}      — per-task per-run per-hemi surface BOLD (discovered)

─── To change expected outputs ──────────────────────────────────────────────
[DEV] Edit MASKINFO_SUFFIXES_HEMI_L / _HEMI_R, EXPECTED_RUNS, or BOLD_HEMIS.
      No changes needed in the engine or registry.
"""

from __future__ import annotations

import re
from pathlib import Path

from .check_analysis_integrity import AnalysisSpec
from .check_analysis_integrity import default_combinations


class PRFPrepareSpec(AnalysisSpec):
    # ── Maskinfo: explicit per-hemi suffix lists (ROI names differ L vs R) ──
    # [DEV] Add / remove ROI entries here as atlases change
    MASKINFO_SUFFIXES_HEMI_L = [
        "_hemi-L_desc-hFEF-wang_maskinfo.json",
        "_hemi-L_desc-hV4-benson_maskinfo.json",
        "_hemi-L_desc-hV4-wang_maskinfo.json",
        "_hemi-L_desc-IPS0-wang_maskinfo.json",
        "_hemi-L_desc-IPS1-wang_maskinfo.json",
        "_hemi-L_desc-IPS2-wang_maskinfo.json",
        "_hemi-L_desc-IPS3-wang_maskinfo.json",
        "_hemi-L_desc-IPS4-wang_maskinfo.json",
        "_hemi-L_desc-IPS5-wang_maskinfo.json",
        "_hemi-L_desc-lh.G_oc-temp_lat-fusifor-prf_VOTC_maskinfo.json",
        "_hemi-L_desc-lh.G_temporal_inf-prf_VOTC_maskinfo.json",
        "_hemi-L_desc-lh.S_oc-temp_lat-prf_VOTC_maskinfo.json",
        "_hemi-L_desc-LO1-benson_maskinfo.json",
        "_hemi-L_desc-LO1-wang_maskinfo.json",
        "_hemi-L_desc-LO2-benson_maskinfo.json",
        "_hemi-L_desc-LO2-wang_maskinfo.json",
        "_hemi-L_desc-PHC1-wang_maskinfo.json",
        "_hemi-L_desc-PHC2-wang_maskinfo.json",
        "_hemi-L_desc-SPL1-wang_maskinfo.json",
        "_hemi-L_desc-TO1-benson_maskinfo.json",
        "_hemi-L_desc-TO1-wang_maskinfo.json",
        "_hemi-L_desc-TO2-benson_maskinfo.json",
        "_hemi-L_desc-TO2-wang_maskinfo.json",
        "_hemi-L_desc-V1-benson_maskinfo.json",
        "_hemi-L_desc-V1d-wang_maskinfo.json",
        "_hemi-L_desc-V1v-wang_maskinfo.json",
        "_hemi-L_desc-V2-benson_maskinfo.json",
        "_hemi-L_desc-V2d-wang_maskinfo.json",
        "_hemi-L_desc-V2v-wang_maskinfo.json",
        "_hemi-L_desc-V3a-benson_maskinfo.json",
        "_hemi-L_desc-V3a-wang_maskinfo.json",
        "_hemi-L_desc-V3b-benson_maskinfo.json",
        "_hemi-L_desc-V3-benson_maskinfo.json",
        "_hemi-L_desc-V3b-wang_maskinfo.json",
        "_hemi-L_desc-V3d-wang_maskinfo.json",
        "_hemi-L_desc-V3v-wang_maskinfo.json",
        "_hemi-L_desc-VO1-benson_maskinfo.json",
        "_hemi-L_desc-VO1-wang_maskinfo.json",
        "_hemi-L_desc-VO2-benson_maskinfo.json",
        "_hemi-L_desc-VO2-wang_maskinfo.json",
    ]

    MASKINFO_SUFFIXES_HEMI_R = [
        "_hemi-R_desc-G_oc-temp_lat-fusifor-prf_VOTC_maskinfo.json",
        "_hemi-R_desc-G_temporal_inf-prf_VOTC_maskinfo.json",
        "_hemi-R_desc-hFEF-wang_maskinfo.json",
        "_hemi-R_desc-hV4-benson_maskinfo.json",
        "_hemi-R_desc-hV4-wang_maskinfo.json",
        "_hemi-R_desc-IPS0-wang_maskinfo.json",
        "_hemi-R_desc-IPS1-wang_maskinfo.json",
        "_hemi-R_desc-IPS2-wang_maskinfo.json",
        "_hemi-R_desc-IPS3-wang_maskinfo.json",
        "_hemi-R_desc-IPS4-wang_maskinfo.json",
        "_hemi-R_desc-IPS5-wang_maskinfo.json",
        "_hemi-R_desc-LO1-benson_maskinfo.json",
        "_hemi-R_desc-LO1-wang_maskinfo.json",
        "_hemi-R_desc-LO2-benson_maskinfo.json",
        "_hemi-R_desc-LO2-wang_maskinfo.json",
        "_hemi-R_desc-PHC1-wang_maskinfo.json",
        "_hemi-R_desc-PHC2-wang_maskinfo.json",
        "_hemi-R_desc-S_oc-temp_lat-prf_VOTC_maskinfo.json",
        "_hemi-R_desc-SPL1-wang_maskinfo.json",
        "_hemi-R_desc-TO1-benson_maskinfo.json",
        "_hemi-R_desc-TO1-wang_maskinfo.json",
        "_hemi-R_desc-TO2-benson_maskinfo.json",
        "_hemi-R_desc-TO2-wang_maskinfo.json",
        "_hemi-R_desc-V1-benson_maskinfo.json",
        "_hemi-R_desc-V1d-wang_maskinfo.json",
        "_hemi-R_desc-V1v-wang_maskinfo.json",
        "_hemi-R_desc-V2-benson_maskinfo.json",
        "_hemi-R_desc-V2d-wang_maskinfo.json",
        "_hemi-R_desc-V2v-wang_maskinfo.json",
        "_hemi-R_desc-V3a-benson_maskinfo.json",
        "_hemi-R_desc-V3a-wang_maskinfo.json",
        "_hemi-R_desc-V3b-benson_maskinfo.json",
        "_hemi-R_desc-V3-benson_maskinfo.json",
        "_hemi-R_desc-V3b-wang_maskinfo.json",
        "_hemi-R_desc-V3d-wang_maskinfo.json",
        "_hemi-R_desc-V3v-wang_maskinfo.json",
        "_hemi-R_desc-VO1-benson_maskinfo.json",
        "_hemi-R_desc-VO1-wang_maskinfo.json",
        "_hemi-R_desc-VO2-benson_maskinfo.json",
        "_hemi-R_desc-VO2-wang_maskinfo.json",
    ]

    EXPECTED_RUNS = ["01", "02", "0102avg"]
    BOLD_HEMIS = ["hemi-L", "hemi-R"]

    @property
    def name(self) -> str:
        return "prfprepare"

    @property
    def description(self) -> str:
        n_mask = len(self.MASKINFO_SUFFIXES_HEMI_L) + len(self.MASKINFO_SUFFIXES_HEMI_R)
        return (
            f"PRF prepare outputs — {n_mask} maskinfo (fixed) + "
            f"events & bold per discovered task"
        )

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses / "func"

    def _discover_tasks(
        self,
        session_dir: Path,
        prefix: str,
    ) -> tuple[set[str], set[str]]:
        """Scan directory to discover which event tasks and bold tasks exist."""
        event_tasks: set[str] = set()
        bold_tasks: set[str] = set()
        task_pattern = re.compile(re.escape(prefix) + r"_task-([^_]+)_run-")
        for f in session_dir.iterdir():
            if not f.is_file():
                continue
            m = task_pattern.match(f.name)
            if not m:
                continue
            task = m.group(1)
            if f.name.endswith("_events.tsv"):
                event_tasks.add(task)
            elif f.name.endswith("_bold.nii.gz"):
                bold_tasks.add(task)
        return event_tasks, bold_tasks

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        # session_dir = .../sub-XX/ses-XX/func/
        sub = session_dir.parent.parent.name
        ses = session_dir.parent.name
        prefix = f"{sub}_{ses}"
        groups: dict[str, list[str]] = {}

        # Maskinfo (always fixed)
        groups["maskinfo"] = [
            f"{prefix}{s}"
            for s in self.MASKINFO_SUFFIXES_HEMI_L + self.MASKINFO_SUFFIXES_HEMI_R
        ]

        event_tasks, bold_tasks = self._discover_tasks(session_dir, prefix)

        for task in sorted(event_tasks):
            groups[f"events-{task}"] = [
                f"{prefix}_task-{task}_run-{run}_events.tsv"
                for run in self.EXPECTED_RUNS
            ]

        for task in sorted(bold_tasks):
            groups[f"bold-{task}"] = [
                f"{prefix}_task-{task}_run-{run}_{hemi}_bold.nii.gz"
                for run in self.EXPECTED_RUNS
                for hemi in self.BOLD_HEMIS
            ]

        return groups

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()
