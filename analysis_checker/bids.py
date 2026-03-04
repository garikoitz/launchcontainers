"""
analysis_specs/bids.py
=======================
BIDS-related specs:
  BIDSSpec            — raw BIDS modality folder check (glob-based)
  BIDSDWISpec         — BIDS DWI acq-label discovery + AP/PA file check
  BIDSFuncSBRefSpec   — sbref ↔ bold pairing + timing gap check
  BIDSScanstsvSpec    — scans.tsv ↔ JSON AcquisitionTime consistency check

─── To change expected patterns ─────────────────────────────────────────────
[DEV] Edit the MODALITY_PATTERNS, AP_EXTENSIONS, PA_EXTENSIONS, or
      _tsv_keys_for() mapping inside the relevant class.
      No changes needed in the engine or registry.
"""

from __future__ import annotations

import csv as _csv
import re
from datetime import datetime
from pathlib import Path

from .check_analysis_integrity import AnalysisSpec
from .check_analysis_integrity import default_combinations
from .check_analysis_integrity import json_for
from .check_analysis_integrity import parse_hms
from .check_analysis_integrity import read_json
from .check_analysis_integrity import times_match


# =============================================================================
# BIDSSpec
# =============================================================================


class BIDSSpec(AnalysisSpec):
    """
    Raw BIDS: analysis_dir / sub-XX / ses-XX / {anat,func,fmap,dwi} / <files>
    Files grouped by modality subfolder, using glob patterns.

    [DEV] Add / remove modalities or glob patterns in MODALITY_PATTERNS.
    """

    MODALITY_PATTERNS: dict[str, list[str]] = {
        "anat": ["{sub}_{ses}_*T1w.nii.gz"],
        "func": [
            "{sub}_{ses}_task-*_bold.nii.gz",
            "{sub}_{ses}_task-*_bold.json",
            "{sub}_{ses}_task-*_events.tsv",
        ],
        "fmap": ["{sub}_{ses}_*epi.nii.gz"],
        "dwi": ["{sub}_{ses}_*dwi.nii.gz"],
    }

    @property
    def name(self) -> str:
        return "bids"

    @property
    def description(self) -> str:
        return "Raw BIDS dataset — anat, func, fmap, dwi"

    @property
    def uses_glob(self) -> bool:
        return True

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses

    def get_expected_subfolders(
        self,
        analysis_dir: Path,
        sub: str,
        ses: str,
    ) -> list[Path]:
        session = analysis_dir / sub / ses
        return [session / mod for mod in self.MODALITY_PATTERNS]

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        sub = session_dir.parent.name
        ses = session_dir.name
        return {
            modality: [
                f"{modality}/{pat.replace('{sub}', sub).replace('{ses}', ses)}"
                for pat in patterns
            ]
            for modality, patterns in self.MODALITY_PATTERNS.items()
        }

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()


# =============================================================================
# BIDSDWISpec
# =============================================================================


class BIDSDWISpec(AnalysisSpec):
    """
    BIDS DWI: discovers acq- labels on disk, then for each acq expects:
      dir-AP_run-01:  .bval, .bvec, .json, .nii.gz
      dir-PA_run-01:  .json, .nii.gz

    [DEV] Edit AP_EXTENSIONS or PA_EXTENSIONS to change expected file types.
    """

    AP_EXTENSIONS = [".bval", ".bvec", ".json", ".nii.gz"]
    PA_EXTENSIONS = [".json", ".nii.gz"]

    @property
    def name(self) -> str:
        return "bidsdwi"

    @property
    def description(self) -> str:
        return (
            f"BIDS DWI — per acq: "
            f"AP ({len(self.AP_EXTENSIONS)} files) + PA ({len(self.PA_EXTENSIONS)} files)"
        )

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses / "dwi"

    def get_expected_subfolders(
        self,
        analysis_dir: Path,
        sub: str,
        ses: str,
    ) -> list[Path]:
        return [analysis_dir / sub / ses / "dwi"]

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        if not session_dir.is_dir():
            return {}

        # session_dir = .../sub-XX/ses-XX/dwi/
        sub = session_dir.parent.parent.name
        ses = session_dir.parent.name
        prefix = f"{sub}_{ses}"

        acq_pattern = re.compile(re.escape(prefix) + r"_acq-([^_]+)_dir-")
        acq_labels: set[str] = set()
        for f in session_dir.iterdir():
            if f.is_file():
                m = acq_pattern.match(f.name)
                if m:
                    acq_labels.add(m.group(1))

        groups: dict[str, list[str]] = {}
        for acq in sorted(acq_labels):
            expected = [
                f"{prefix}_acq-{acq}_dir-AP_run-01_dwi{ext}"
                for ext in self.AP_EXTENSIONS
            ] + [
                f"{prefix}_acq-{acq}_dir-PA_run-01_dwi{ext}"
                for ext in self.PA_EXTENSIONS
            ]
            groups[f"acq-{acq}"] = expected

        return groups

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()


# =============================================================================
# BIDSFuncSBRefSpec
# =============================================================================


class BIDSFuncSBRefSpec(AnalysisSpec):
    """
    BIDS func sbref ↔ bold pairing check.

    For every bold.nii.gz (non-symlink) in func/, checks:
      1. Matching sbref.nii.gz exists
      2. Both have a .json sidecar
      3. AcquisitionTime gap between sbref and bold <= max_diff_sec

    [DEV] Adjust max_diff_sec if your protocol has a longer SBRef→bold gap.
    """

    def __init__(self, max_diff_sec: int = 30):
        self.max_diff_sec = max_diff_sec

    @property
    def name(self) -> str:
        return "bidsfuncsbref"

    @property
    def description(self) -> str:
        return f"BIDS func sbref↔bold pairing — timing gap <= {self.max_diff_sec}s"

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses / "func"

    def get_expected_subfolders(
        self,
        analysis_dir: Path,
        sub: str,
        ses: str,
    ) -> list[Path]:
        return [self.get_session_dir(analysis_dir, sub, ses)]

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        if not session_dir.is_dir():
            return {}

        groups: dict[str, list[str]] = {}
        for bold in sorted(
            f
            for f in session_dir.iterdir()
            if f.is_file() and not f.is_symlink() and f.name.endswith("_bold.nii.gz")
        ):
            sbref = Path(bold.name.replace("_bold.nii.gz", "_sbref.nii.gz"))
            bold_json = Path(bold.name.replace(".nii.gz", ".json"))
            sbref_json = Path(sbref.name.replace(".nii.gz", ".json"))
            groups[bold.name] = [str(sbref), str(bold_json), str(sbref_json)]

        return groups

    def _check_timing(self, session_dir: Path, bold_name: str) -> str | None:
        """
        Returns a diagnostic string if sbref↔bold timing gap exceeds threshold.
        Called by check_one_session() via duck-typing (hasattr check).
        """
        sbref_json = session_dir / bold_name.replace("_bold.nii.gz", "_sbref.json")
        bold_json = session_dir / bold_name.replace(".nii.gz", ".json")

        t_sbref = read_json(sbref_json).get("AcquisitionTime")
        t_bold = read_json(bold_json).get("AcquisitionTime")

        if t_sbref is None or t_bold is None:
            return f"  AcquisitionTime missing (sbref={t_sbref}, bold={t_bold})"

        if not times_match(t_sbref, t_bold, self.max_diff_sec):
            try:
                dt1 = datetime.strptime(parse_hms(t_sbref), "%H:%M:%S")
                dt2 = datetime.strptime(parse_hms(t_bold), "%H:%M:%S")
                diff = abs((dt1 - dt2).total_seconds())
            except ValueError:
                diff = -1
            return (
                f"  timing mismatch: sbref={parse_hms(t_sbref)}, "
                f"bold={parse_hms(t_bold)}, gap={diff:.0f}s (max={self.max_diff_sec}s)"
            )
        return None

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()


# =============================================================================
# BIDSScanstsvSpec
# =============================================================================


class BIDSScanstsvSpec(AnalysisSpec):
    """
    BIDS scans.tsv ↔ JSON AcquisitionTime consistency check.

    Mapping rules (BIDS file → expected scans.tsv entry):
      anat/*_T1w.nii.gz   → _T1_inv1, _T1_inv2, _T1_uni  (3 rows)
      anat/*_T2w.nii.gz   → same name
      fmap/*_epi.nii.gz   → same name (run single-digit in tsv)
      func/*_bold.nii.gz  → _magnitude + _phase
      func/*_sbref.nii.gz → same name

    [DEV] Edit _tsv_keys_for() to change the BIDS file → scans.tsv mapping.
    [DEV] Edit _collect_bids_files() to add/remove modalities to check.
    """

    def __init__(self, max_diff_sec: int = 30):
        self.max_diff_sec = max_diff_sec

    @property
    def name(self) -> str:
        return "bidsscantsv"

    @property
    def description(self) -> str:
        return (
            f"BIDS scans.tsv ↔ JSON AcquisitionTime consistency "
            f"(T1w, T2w, fmap, bold, sbref) — gap <= {self.max_diff_sec}s"
        )

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        sub_str = sub if sub.startswith("sub-") else f"sub-{sub}"
        ses_str = ses if ses.startswith("ses-") else f"ses-{ses}"
        return analysis_dir / sub_str / ses_str

    def get_expected_subfolders(
        self,
        analysis_dir: Path,
        sub: str,
        ses: str,
    ) -> list[Path]:
        return [self.get_session_dir(analysis_dir, sub, ses)]

    @property
    def uses_glob(self) -> bool:
        return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _read_scans_tsv(self, session_dir: Path) -> dict[str, str]:
        """Read scans.tsv → {filename: AcquisitionTime}."""
        tsv_files = list(session_dir.glob("*_scans.tsv"))
        if not tsv_files:
            return {}
        result: dict[str, str] = {}
        try:
            with open(tsv_files[0]) as f:
                for row in _csv.DictReader(f, delimiter="\t"):
                    fname = row.get("filename", "").strip()
                    acqtime = row.get("acq_time", "").strip()
                    if fname:
                        result[fname] = acqtime
        except Exception:
            pass
        return result

    def _tsv_keys_for(self, bids_file: Path, sub: str, ses: str) -> list[str]:
        """
        Map a BIDS nii.gz → list of expected scans.tsv filename entries.

        [DEV] Edit the mapping rules here if your scans.tsv uses different
              naming conventions (e.g. different run normalisation).
        """
        name = bids_file.name
        modality = bids_file.parent.name
        stem = name.replace(".nii.gz", "")

        if name.endswith("_T1w.nii.gz"):
            base = stem.replace("_T1w", "")
            return [
                f"anat/{base}_T1_inv1.nii.gz",
                f"anat/{base}_T1_inv2.nii.gz",
                f"anat/{base}_T1_uni.nii.gz",
            ]

        if name.endswith("_T2w.nii.gz"):
            return [f"anat/{name}"]

        if modality == "fmap" and name.endswith("_epi.nii.gz"):
            normalised = re.sub(r"run-0*(\d+)", lambda m: f"run-{m.group(1)}", name)
            return [f"fmap/{normalised}"]

        if name.endswith("_bold.nii.gz"):
            return [
                f"func/{name.replace('_bold.nii.gz', '_magnitude.nii.gz')}",
                f"func/{name.replace('_bold.nii.gz', '_phase.nii.gz')}",
            ]

        if name.endswith("_sbref.nii.gz"):
            return [f"func/{name}"]

        return []

    def _collect_bids_files(self, session_dir: Path) -> list[Path]:
        """
        Collect all non-symlink nii.gz files to check.

        [DEV] Add new modality/pattern pairs here to extend coverage.
        """
        patterns = {
            "anat": ["*_T1w.nii.gz", "*_T2w.nii.gz"],
            "fmap": ["*_epi.nii.gz"],
            "func": ["*_bold.nii.gz", "*_sbref.nii.gz"],
        }
        files: list[Path] = []
        for modality, globs in patterns.items():
            mod_dir = session_dir / modality
            if not mod_dir.is_dir():
                continue
            for pattern in globs:
                files.extend(
                    f for f in sorted(mod_dir.glob(pattern)) if not f.is_symlink()
                )
        return files

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        if not session_dir.is_dir():
            return {}

        sub = session_dir.parent.name
        ses = session_dir.name
        tsv = self._read_scans_tsv(session_dir)

        if not tsv:
            return {"scans.tsv": ["*_scans.tsv"]}

        groups: dict[str, list[str]] = {}
        for bids_file in self._collect_bids_files(session_dir):
            rel = str(bids_file.relative_to(session_dir))
            # Only missing tsv keys become "expected" — already-present ones
            # produce an empty list → engine marks that group complete
            missing_in_tsv = [
                k for k in self._tsv_keys_for(bids_file, sub, ses) if k not in tsv
            ]
            groups[rel] = missing_in_tsv

        return groups

    def _check_timing(self, session_dir: Path, group_label: str) -> str | None:
        """
        Compare JSON AcquisitionTime against scans.tsv acq_time.
        Called by check_one_session() via duck-typing.
        """
        sub = session_dir.parent.name
        ses = session_dir.name
        tsv = self._read_scans_tsv(session_dir)

        bids_file = session_dir / group_label
        if not bids_file.exists():
            return None

        json_meta = (
            read_json(json_for(bids_file)) if json_for(bids_file).exists() else {}
        )
        t_json_raw = json_meta.get("AcquisitionTime")
        t_json = parse_hms(t_json_raw) if t_json_raw else None

        issues = []
        for key in self._tsv_keys_for(bids_file, sub, ses):
            t_tsv_raw = tsv.get(key)
            if t_tsv_raw is None:
                continue
            t_tsv = parse_hms(t_tsv_raw) if t_tsv_raw else None
            if t_json is None or t_tsv is None:
                continue
            if not times_match(t_json, t_tsv, self.max_diff_sec):
                try:
                    dt1 = datetime.strptime(t_json, "%H:%M:%S")
                    dt2 = datetime.strptime(t_tsv, "%H:%M:%S")
                    diff = abs((dt1 - dt2).total_seconds())
                except Exception:
                    diff = -1
                issues.append(
                    f"  {key}: json={t_json or 'missing'}, "
                    f"tsv={t_tsv or 'missing'}, gap={diff:.0f}s",
                )

        return "\n".join(issues) if issues else None

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()
