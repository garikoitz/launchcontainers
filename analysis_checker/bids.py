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
import fnmatch
import re
from datetime import datetime
from pathlib import Path

try:
    from .base import (
        AnalysisSpec,
        default_combinations,
        json_for,
        parse_hms,
        read_json,
        times_match,
        WC_SESSIONS,
    )
except ImportError:
    import sys
    import os

    sys.path.insert(0, os.path.dirname(__file__))
    from base import (
        AnalysisSpec,
        default_combinations,
        json_for,
        parse_hms,
        read_json,
        times_match,
        WC_SESSIONS,
    )


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


class DWINiiSpec(AnalysisSpec):
    """
    BIDS DWI: expects two fixed acq groups — acq-magonly and acq-nordic.
    Each has:
      dir-AP_run-01:  .bval, .bvec, .json, .nii.gz
      dir-PA_run-01:  .json, .nii.gz
    """

    ACQ_LABELS = ["magonly", "nordic"]
    AP_EXTENSIONS = [".bval", ".bvec", ".json", ".nii.gz"]
    PA_EXTENSIONS = [".json", ".nii.gz"]

    @property
    def name(self) -> str:
        return "bidsdwi"

    @property
    def description(self) -> str:
        return (
            f"BIDS DWI — acq-magonly + acq-nordic, "
            f"AP ({len(self.AP_EXTENSIONS)} files) + PA ({len(self.PA_EXTENSIONS)} files) each"
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

        sub = session_dir.parent.parent.name
        ses = session_dir.parent.name
        prefix = f"{sub}_{ses}"

        groups: dict[str, list[str]] = {}
        for acq in self.ACQ_LABELS:
            groups[f"acq-{acq}"] = [
                f"{prefix}_acq-{acq}_dir-AP_run-01_dwi{ext}"
                for ext in self.AP_EXTENSIONS
            ] + [
                f"{prefix}_acq-{acq}_dir-PA_run-01_dwi{ext}"
                for ext in self.PA_EXTENSIONS
            ]

        return groups

    def get_group_dimension(self, group_label: str) -> tuple[str, str] | None:
        if group_label.startswith("acq-"):
            return ("acq", group_label.split("acq-")[-1])
        return None

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()


# =============================================================================
# BIDSFuncSBRefSpec
# =============================================================================


class FuncSBRefSpec(AnalysisSpec):
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


class ScanstsvSpec(AnalysisSpec):
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


# =============================================================================
# BIDSfuncSpec
# =============================================================================


class BIDSfuncSpec(AnalysisSpec):
    """
    VOTCLOC func integrity check.

    Expected per sub/ses (16 total groups):

    * ``task-fLoc``  — 10 runs (run-01 … run-10)
    * ``task-retRW`` — 2 runs  (run-01, run-02)
    * ``task-retFF`` — 2 runs  (run-01, run-02)
    * ``task-retCB`` — 2 runs  (run-01, run-02)

    For every group the following four files are expected:

    * ``*_task-<X>_run-<N>_bold.nii.gz``
    * ``*_task-<X>_run-<N>_bold.json``
    * ``*_task-<X>_run-<N>_sbref.nii.gz``
    * ``*_task-<X>_run-<N>_sbref.json``

    Timing check: ``AcquisitionTime`` gap between sbref and bold <= ``max_diff_sec``.

    [DEV] Adjust FLOC_RUNS or RET_TASKS_RUNS to change expected counts.
    """

    FLOC_RUNS: list[str] = [f"{i:02d}" for i in range(1, 11)]  # 01–10
    RET_TASKS_RUNS: dict[str, list[str]] = {
        "retRW": ["01", "02"],
        "retFF": ["01", "02"],
        "retCB": ["01", "02"],
    }

    # [DEV] Sessions that only acquired retRW + retCB (no retFF) → 14 expected groups.
    REDUCED_RET_SESSIONS: set[tuple[str, str]] = {
        ("sub-03", "ses-01"),
        ("sub-04", "ses-01"),
        ("sub-06", "ses-01"),
    }

    # [DEV] Add task name patterns here to extend monitoring to new tasks.
    # Supports shell wildcards: "ret*" matches retRW, retFF, retCB, retfix*, etc.
    TASKS_OF_INTEREST: list[str] = ["fLoc", "ret*"]

    # WC session expected tasks are discovered dynamically from vistadisplog
    # (see _discover_wc_ret_tasks). Add new retfix variants there automatically.

    def __init__(self, max_diff_sec: int = 30):
        self.max_diff_sec = max_diff_sec

    def _task_of_interest(self, task: str) -> bool:
        """Return True if *task* matches any pattern in TASKS_OF_INTEREST."""
        return any(fnmatch.fnmatch(task, pat) for pat in self.TASKS_OF_INTEREST)

    def _is_wc_session(self, sub: str, ses: str) -> bool:
        """Return True if this sub/ses is a WC (retfix) session."""
        sub_n = sub.replace("sub-", "").zfill(2)
        ses_n = ses.replace("ses-", "").zfill(2)
        return (sub_n, ses_n) in WC_SESSIONS

    def _discover_wc_ret_tasks(self, session_dir: Path) -> dict[str, list[str]]:
        """
        Scan vistadisplog for this WC session and return {task: [run, ...]}
        for all retfix* tasks found in *_params.mat filenames.

        session_dir is BIDS/sub-XX/ses-XX/func, so the vistadisplog is at
        BIDS/sourcedata/vistadisplog/sub-XX/ses-XX/.
        """
        bids_root = session_dir.parent.parent.parent
        sub = session_dir.parent.parent.name  # sub-XX
        ses = session_dir.parent.name  # ses-XX
        displog_dir = bids_root / "sourcedata" / "vistadisplog" / sub / ses

        tasks: dict[str, list[str]] = {}
        if not displog_dir.is_dir():
            return tasks

        for mat in sorted(displog_dir.glob("*_params.mat")):
            m = re.search(r"task-(\w+)_run-(\d+)", mat.name)
            if not m:
                continue
            task, run = m.group(1), m.group(2)
            if not task.startswith("retfix"):
                continue
            tasks.setdefault(task, [])
            if run not in tasks[task]:
                tasks[task].append(run)

        # sort runs within each task
        for task in tasks:
            tasks[task] = sorted(tasks[task])
        return tasks

    def _ret_tasks_for_session(self, sub: str, ses: str) -> dict[str, list[str]]:
        """Return expected ret task → runs mapping for this session.

        Reduced sessions (sub-03/04/06 ses-01) only have retRW + retCB.
        """
        sub_str = sub if sub.startswith("sub-") else f"sub-{sub}"
        ses_str = ses if ses.startswith("ses-") else f"ses-{ses}"
        if (sub_str, ses_str) in self.REDUCED_RET_SESSIONS:
            return {
                k: v for k, v in self.RET_TASKS_RUNS.items() if k in ("retRW", "retCB")
            }
        return self.RET_TASKS_RUNS

    @property
    def name(self) -> str:
        return "funcintegrity"

    @property
    def description(self) -> str:
        n_floc = len(self.FLOC_RUNS)
        n_ret = sum(len(v) for v in self.RET_TASKS_RUNS.values())
        n_wc = len(WC_SESSIONS)
        return (
            f"VOTCLOC func integrity — fLoc ({n_floc} runs) + "
            f"ret tasks ({n_ret} runs) = {n_floc + n_ret} groups (normal); "
            f"WC sessions ({n_wc}): retfix tasks discovered from vistadisplog; "
            f"timing gap <= {self.max_diff_sec}s"
        )

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        sub_str = sub if sub.startswith("sub-") else f"sub-{sub}"
        ses_str = ses if ses.startswith("ses-") else f"ses-{ses}"
        return analysis_dir / sub_str / ses_str / "func"

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

        sub = session_dir.parent.parent.name  # sub-XX
        ses = session_dir.parent.name  # ses-XX
        prefix = f"{sub}_{ses}"

        groups: dict[str, list[str]] = {}

        def _files_for(task: str, run: str) -> list[str]:
            return [
                f"{prefix}_task-{task}_run-{run}_bold.nii.gz",
                f"{prefix}_task-{task}_run-{run}_bold.json",
                f"{prefix}_task-{task}_run-{run}_sbref.nii.gz",
                f"{prefix}_task-{task}_run-{run}_sbref.json",
            ]

        # fLoc runs — same for both normal and WC sessions
        for run in self.FLOC_RUNS:
            groups[f"task-fLoc_run-{run}"] = _files_for("fLoc", run)

        if self._is_wc_session(sub, ses):
            # ── WC session: expected ret tasks from vistadisplog params.mat ──
            wc_tasks = self._discover_wc_ret_tasks(session_dir)
            for task, runs in wc_tasks.items():
                for run in runs:
                    groups[f"task-{task}_run-{run}"] = _files_for(task, run)

            # Scan BIDS func for retfix* runs not found in vistadisplog → EXTRA
            expected_labels = set(groups.keys())
            for bold in sorted(
                session_dir.glob(f"{prefix}_task-retfix*_run-*_bold.nii.gz")
            ):
                m = re.search(r"task-(\w+)_run-(\d+)", bold.name)
                if not m:
                    continue
                task, run = m.group(1), m.group(2)
                label = f"task-{task}_run-{run}"
                if label not in expected_labels:
                    groups[f"EXTRA:{label}"] = _files_for(task, run)
        else:
            # ── Normal session: fixed ret tasks (retRW/FF/CB, session-specific) ──
            ret_tasks = self._ret_tasks_for_session(sub, ses)
            for task, runs in ret_tasks.items():
                for run in runs:
                    groups[f"task-{task}_run-{run}"] = _files_for(task, run)

            # Scan for extra ret runs (beyond expected 2 per task).
            # fLoc extras are acceptable and NOT flagged.
            expected_labels = set(groups.keys())
            for bold in sorted(session_dir.glob(f"{prefix}_task-*_run-*_bold.nii.gz")):
                m = re.search(r"task-(\w+)_run-(\d+)", bold.name)
                if not m:
                    continue
                task, run = m.group(1), m.group(2)
                if task == "fLoc":
                    continue
                if not self._task_of_interest(task):
                    continue
                label = f"task-{task}_run-{run}"
                if label not in expected_labels:
                    groups[f"EXTRA:{label}"] = _files_for(task, run)

        return groups

    def _check_timing(self, session_dir: Path, group_label: str) -> str | None:
        """
        Check AcquisitionTime gap between sbref and bold for this group.
        Called by the engine via duck-typing (hasattr check).
        Handles both normal labels and ``EXTRA:task-X_run-N`` labels.
        """
        # Strip EXTRA: prefix if present
        label = group_label.removeprefix("EXTRA:")
        if not re.search(r"task-(\w+)_run-(\d+)", label):
            return None

        sub = session_dir.parent.parent.name
        ses = session_dir.parent.name
        prefix = f"{sub}_{ses}"

        sbref_json = session_dir / f"{prefix}_{label}_sbref.json"
        bold_json = session_dir / f"{prefix}_{label}_bold.json"

        t_sbref = (
            read_json(sbref_json).get("AcquisitionTime")
            if sbref_json.exists()
            else None
        )
        t_bold = (
            read_json(bold_json).get("AcquisitionTime") if bold_json.exists() else None
        )

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

    def get_group_dimension(self, group_label: str) -> tuple[str, str] | None:
        # Strip EXTRA: prefix so dimensions still group correctly
        label = group_label.removeprefix("EXTRA:")
        m = re.search(r"task-(\w+)_run-", label)
        return ("task", m.group(1)) if m else None

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()
