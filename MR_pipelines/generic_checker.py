#!/usr/bin/env python3
"""
Check Analysis Integrity (Generic)
===================================
Validates completeness of neuroimaging analysis folders by comparing actual
files against expected file specs defined in AnalysisSpec subclasses.

Each analysis type subclasses AnalysisSpec and defines:
  - Folder structure (get_session_dir, get_expected_subfolders)
  - File grouping logic (get_expected_groups)
  - Default sub/ses combinations (get_default_combinations)

The checker engine reads these specs generically — no analysis-specific logic
in the engine.

Usage:
    python check_analysis_integrity.py /path/to/analysis prfprepare
    python check_analysis_integrity.py /path/to/analysis prfanalyze -s 01,01 -s 01,02
    python check_analysis_integrity.py /path/to/analysis prfanalyze --subseslist-file subseslist.csv
    python check_analysis_integrity.py /path/to/analysis bids --verbose

Output:
    1. <type>_incomplete.csv  — brief sub,ses,RUN for every sub/ses
    2. <type>_detailed.log    — indexed detailed log of missing files
"""
from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Check analysis folder integrity against expected file specs.")
console = Console()


# =============================================================================
# 1. ABSTRACT ANALYSIS SPEC
# =============================================================================


class AnalysisSpec(ABC):
    """
    Abstract base class defining what an analysis type expects.

    Subclass for each analysis type. The checker engine reads everything
    from these methods — zero special-casing in the engine.

    "Groups" are the logical units files are organized by:
      - prfanalyze:  run prefix  (files = prefix × hemi × suffix)
      - prfprepare:  maskinfo | events | bold  (each has own expected file list)
      - bids:        modality subfolder (anat, func, fmap, dwi)
      - fmriprep:    modality subfolder (anat, func)
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short name, e.g. 'prfanalyze', 'prfprepare'."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description."""
        ...

    @property
    def uses_glob(self) -> bool:
        """If True, file patterns may contain * or ? and will be glob-matched."""
        return False

    @abstractmethod
    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        """Return the root directory to scan for this sub/ses."""
        ...

    @abstractmethod
    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        """
        Discover file groups and return expected files for each.

        Returns
        -------
        dict[str, list[str]]
            group_label -> list of expected file paths relative to session_dir
        """
        ...

    def get_expected_subfolders(
        self, analysis_dir: Path, sub: str, ses: str
    ) -> list[Path]:
        """Directories that must exist. Default: just the session dir."""
        return [self.get_session_dir(analysis_dir, sub, ses)]

    def get_default_combinations(self) -> list[tuple[str, str]] | None:
        """Default sub/ses list if no --subseslist given. None = require input."""
        return None


# =============================================================================
# 2. RESULT DATA CLASSES
# =============================================================================


@dataclass
class GroupResult:
    """Result for one file group."""

    group_label: str
    expected_files: list[str]
    found_files: list[str] = field(default_factory=list)
    missing_files: list[str] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return len(self.missing_files) == 0


@dataclass
class SessionResult:
    """Result for one sub/ses pair."""

    sub: str
    ses: str
    session_dir_exists: bool
    missing_folders: list[str] = field(default_factory=list)
    groups: dict[str, GroupResult] = field(default_factory=dict)

    @property
    def num_groups(self) -> int:
        return len(self.groups)

    @property
    def complete_groups(self) -> int:
        return sum(1 for g in self.groups.values() if g.is_complete)

    @property
    def is_complete(self) -> bool:
        return (
            self.session_dir_exists
            and len(self.missing_folders) == 0
            and self.num_groups > 0
            and all(g.is_complete for g in self.groups.values())
        )

    @property
    def total_missing(self) -> int:
        return sum(len(g.missing_files) for g in self.groups.values())


# =============================================================================
# 3. GENERIC CHECKER ENGINE
# =============================================================================


def _check_file(session_dir: Path, fname: str, use_glob: bool) -> bool:
    """Check if a file exists, with optional glob support."""
    if use_glob and ("*" in fname or "?" in fname):
        fpath = Path(fname)
        parent = (
            session_dir / fpath.parent if str(fpath.parent) != "." else session_dir
        )
        return len(list(parent.glob(fpath.name))) > 0 if parent.is_dir() else False
    else:
        return (session_dir / fname).exists()


def check_one_session(
    spec: AnalysisSpec,
    analysis_dir: Path,
    sub: str,
    ses: str,
) -> SessionResult:
    """Check a single sub/ses against the spec."""
    session_dir = spec.get_session_dir(analysis_dir, sub, ses)
    result = SessionResult(
        sub=sub,
        ses=ses,
        session_dir_exists=session_dir.is_dir(),
    )

    # Check subfolders
    for folder in spec.get_expected_subfolders(analysis_dir, sub, ses):
        if not folder.is_dir():
            try:
                rel = str(folder.relative_to(analysis_dir))
            except ValueError:
                rel = str(folder)
            result.missing_folders.append(rel)

    if not result.session_dir_exists:
        return result

    # Check file groups
    for group_label, expected_files in spec.get_expected_groups(session_dir).items():
        found, missing = [], []
        for fname in expected_files:
            if _check_file(session_dir, fname, spec.uses_glob):
                found.append(fname)
            else:
                missing.append(fname)

        result.groups[group_label] = GroupResult(
            group_label=group_label,
            expected_files=expected_files,
            found_files=found,
            missing_files=missing,
        )

    return result


def run_integrity_check(
    spec: AnalysisSpec,
    analysis_dir: Path,
    subses_list: list[tuple[str, str]],
) -> list[SessionResult]:
    """Run integrity check across all sub/ses pairs."""
    results = []
    with typer.progressbar(
        subses_list,
        label=f"Checking {spec.name} integrity",
        show_pos=True,
        length=len(subses_list),
    ) as progress:
        for sub_raw, ses_raw in progress:
            sub = f"sub-{sub_raw}" if not sub_raw.startswith("sub-") else sub_raw
            ses = f"ses-{ses_raw}" if not ses_raw.startswith("ses-") else ses_raw
            results.append(check_one_session(spec, analysis_dir, sub, ses))
    return results


# =============================================================================
# 4. OUTPUT WRITERS
# =============================================================================


def write_brief_csv(results: list[SessionResult], output_path: Path) -> None:
    """Write brief CSV: sub,ses,RUN — row index matches detailed log."""
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sub", "ses", "RUN"])
        for r in results:
            writer.writerow([
                r.sub.replace("sub-", ""),
                r.ses.replace("ses-", ""),
                r.is_complete,
            ])

def write_matrix_csv(results: list[SessionResult], output_path: Path) -> None:
    """
    Write a pivot-table CSV for Google Sheets.

    Rows = subjects, Columns = sessions.
    Values:
      1   = complete (RUN=True)
      0.5 = incomplete but has data (RUN=False, missing files > 0)
      0   = no directory or no groups (RUN=False, missing = —)
    """
    # Collect all unique subs and sessions, preserving order
    subs_seen: dict[str, None] = {}
    ses_seen: dict[str, None] = {}
    lookup: dict[tuple[str, str], float] = {}

    for r in results:
        sub_num = r.sub.replace("sub-", "")
        ses_num = r.ses.replace("ses-", "")
        subs_seen[sub_num] = None
        ses_seen[ses_num] = None

        if r.is_complete:
            value = 1.0
        elif not r.session_dir_exists or r.num_groups == 0:
            value = 0.0
        else:
            # Has data but incomplete (missing files > 0)
            value = 0.5

        lookup[(sub_num, ses_num)] = value

    subs = list(subs_seen.keys())
    sessions = list(ses_seen.keys())

    def _fmt(v: float) -> str:
        if v == 1.0:
            return "1"
        elif v == 0.5:
            return "0.5"
        else:
            return "0"

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        # Header: ,ses-01,ses-02,...  (empty first cell)
        writer.writerow([""] + [f"ses-{s}" for s in sessions])
        for sub in subs:
            row = [f"sub-{sub}"]
            for ses in sessions:
                val = lookup.get((sub, ses), "")
                row.append(_fmt(val) if val != "" else "")
            writer.writerow(row)



def write_detailed_log(
    results: list[SessionResult],
    spec: AnalysisSpec,
    output_path: Path,
) -> None:
    """Write detailed log indexed [0], [1], ... matching CSV rows."""
    with open(output_path, "w") as f:
        f.write(f"# {spec.name.upper()} Analysis Integrity — Detailed Log\n")
        f.write(f"# {spec.description}\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# {'=' * 70}\n\n")

        for idx, r in enumerate(results):
            sub_num = r.sub.replace("sub-", "")
            ses_num = r.ses.replace("ses-", "")
            status = "COMPLETE" if r.is_complete else "INCOMPLETE"

            f.write(f"[{idx}] sub={sub_num}, ses={ses_num} — {status}\n")

            if not r.session_dir_exists:
                f.write("      Session directory does not exist.\n\n")
                continue

            if r.missing_folders:
                f.write("      Missing folders:\n")
                for mf in r.missing_folders:
                    f.write(f"        - {mf}\n")

            if r.num_groups == 0:
                f.write("      No file groups discovered (empty directory?).\n\n")
                continue

            f.write(
                f"      Groups: {r.complete_groups}/{r.num_groups} complete  |  "
                f"Missing files: {r.total_missing}\n"
            )

            for glabel, gresult in sorted(r.groups.items()):
                if gresult.is_complete:
                    continue
                f.write(f"      [{glabel}] — {len(gresult.missing_files)} missing:\n")
                for mf in gresult.missing_files:
                    f.write(f"        - {mf}\n")

            f.write("\n")


# =============================================================================
# 5. RICH CONSOLE OUTPUT
# =============================================================================


def print_summary(results: list[SessionResult], spec: AnalysisSpec) -> None:
    """Print summary table and statistics."""
    n_complete = sum(1 for r in results if r.is_complete)
    n_incomplete = sum(1 for r in results if not r.is_complete)
    n_no_dir = sum(1 for r in results if not r.session_dir_exists)
    n_no_groups = sum(
        1 for r in results if r.session_dir_exists and r.num_groups == 0
    )
    total_groups = sum(r.num_groups for r in results)
    complete_groups = sum(r.complete_groups for r in results)

    table = Table(title=f"{spec.name.upper()} Integrity Summary")
    table.add_column("sub", style="cyan")
    table.add_column("ses", style="cyan")
    table.add_column("RUN", justify="center")
    table.add_column("Groups", justify="center")
    table.add_column("Missing", justify="center")

    for r in results:
        style = "green" if r.is_complete else "red bold"
        if not r.session_dir_exists:
            groups_str, missing_str = "no dir", "—"
        elif r.num_groups == 0:
            groups_str, missing_str = "0", "—"
        else:
            groups_str = f"{r.complete_groups}/{r.num_groups}"
            missing_str = str(r.total_missing) if r.total_missing > 0 else "0"

        table.add_row(
            r.sub.replace("sub-", ""),
            r.ses.replace("ses-", ""),
            f"[{style}]{r.is_complete}[/{style}]",
            groups_str,
            missing_str,
        )

    console.print(table)
    console.print(
        f"\n[green]{n_complete} complete[/green] | "
        f"[red]{n_incomplete} incomplete[/red] "
        f"out of {len(results)} sessions"
    )
    if total_groups > 0:
        console.print(f"Groups: {complete_groups}/{total_groups} complete overall")
    if n_no_dir > 0:
        console.print(f"[yellow]Missing session dirs: {n_no_dir}[/yellow]")
    if n_no_groups > 0:
        console.print(f"[yellow]Empty session dirs: {n_no_groups}[/yellow]")


def print_detailed_results(
    results: list[SessionResult], verbose: bool = False
) -> None:
    """Print missing file details for incomplete sessions."""
    incomplete = [r for r in results if not r.is_complete]
    if not incomplete:
        console.print("\n[bold green]✓ All sessions complete![/bold green]")
        return

    console.print(f"\n[bold red]Incomplete Sessions ({len(incomplete)}):[/bold red]\n")
    for r in incomplete:
        if not r.session_dir_exists:
            console.print(f"  [yellow]{r.sub}/{r.ses}[/yellow] — directory missing")
            continue
        if r.num_groups == 0:
            console.print(
                f"  [yellow]{r.sub}/{r.ses}[/yellow] — no file groups found"
            )
            continue

        console.print(
            f"  [cyan]{r.sub}/{r.ses}[/cyan] "
            f"({r.complete_groups}/{r.num_groups} groups complete, "
            f"{r.total_missing} files missing)"
        )
        for glabel, gresult in sorted(r.groups.items()):
            if gresult.is_complete:
                if verbose:
                    console.print(f"    [green]✓ {glabel}[/green]")
                continue
            console.print(
                f"    [red]✗ {glabel}[/red] — {len(gresult.missing_files)} missing:"
            )
            for mf in gresult.missing_files:
                console.print(f"      - {mf}")
    console.print()


def print_group_distribution(results: list[SessionResult]) -> None:
    """Print distribution of groups per session."""
    counts = defaultdict(int)
    for r in results:
        if r.num_groups > 0:
            counts[r.num_groups] += 1
    if not counts:
        return

    console.print("\n[bold]Group Distribution:[/bold]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("# Groups", style="cyan")
    table.add_column("# Sessions", style="green")
    for n in sorted(counts.keys()):
        table.add_row(str(n), str(counts[n]))
    console.print(table)


# =============================================================================
# 6. SHARED CONSTANTS — Excluded sessions used by multiple specs
# =============================================================================

EXCLUDED_SESSIONS: set[tuple[str, str]] = {}


def default_combinations() -> list[tuple[str, str]]:
    """All 11 subs × 10 sessions minus excluded."""
    combos = []
    for sub_id in range(1, 12):
        for ses_id in range(1, 11):
            sub = f"sub-{sub_id:02d}"
            ses = f"ses-{ses_id:02d}"
            if (sub, ses) not in EXCLUDED_SESSIONS:
                combos.append((sub, ses))
    return combos


# =============================================================================
# 7. ANALYSIS SPEC: PRFAnalyze (was "prf")
# =============================================================================


class PRFAnalyzeSpec(AnalysisSpec):
    """
    PRF analyze results: analysis_dir / sub-XX / ses-XX / <files>

    Files grouped by run prefix (discovered from files on disk).
    Each run × hemi expects a fixed set of output suffixes.
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
        """Discover run prefixes, build expected = prefix × hemi × suffix."""
        if not session_dir.is_dir():
            return {}

        prefixes: set[str] = set()
        for f in session_dir.iterdir():
            if f.is_file() and "_hemi-" in f.name:
                prefixes.add(f.name.split("_hemi-")[0])

        groups: dict[str, list[str]] = {}
        for prefix in sorted(prefixes):
            expected = []
            for hemi in self.HEMIS:
                for suffix in self.EXPECTED_SUFFIXES:
                    expected.append(f"{prefix}_{hemi}_{suffix}")
            groups[prefix] = expected

        return groups
    
    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()    


# =============================================================================
# 8. ANALYSIS SPEC: PRFPrepare
# =============================================================================


class PRFPrepareSpec(AnalysisSpec):
    """
    PRF prepare outputs: analysis_dir / sub-XX / ses-XX / <files>

    Three groups of files, each with different structure:
      1. maskinfo: per-hemi ROI mask JSON files (hemi-L and hemi-R have
         DIFFERENT ROI names due to lh./rh. prefix on VOTC parcels)
      2. events:   per-task per-run event TSVs (no hemi dimension)
      3. bold:     per-task per-run per-hemi surface BOLD (only "fix" tasks)

    All files are prefixed with {sub}_{ses} on disk.
    """

    # -- Maskinfo: explicit per-hemi suffix lists (ROI names differ!) --
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

    # -- expected runs --
    EXPECTED_RUNS = ["01", "02", "0102avg"]

    # -- hemi for bold files --
    BOLD_HEMIS = ["hemi-L", "hemi-R"]

    @property
    def name(self) -> str:
        return "prfprepare"

    @property
    def description(self) -> str:
        n_mask = len(self.MASKINFO_SUFFIXES_HEMI_L) + len(
            self.MASKINFO_SUFFIXES_HEMI_R
        )
        return (
            f"PRF prepare outputs — {n_mask} maskinfo (fixed) + "
            f"events & bold per discovered task"
        )

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses / "func"
    
    def _discover_tasks(
        self, session_dir: Path, prefix: str
    ) -> tuple[set[str], set[str]]:
        """
        Scan directory to discover which tasks exist.

        Returns
        -------
        event_tasks : set[str]
            Tasks that have at least one _events.tsv file.
        bold_tasks : set[str]
            Tasks that have at least one _bold.nii.gz file.
        """
        import re

        event_tasks: set[str] = set()
        bold_tasks: set[str] = set()

        task_pattern = re.compile(
            re.escape(prefix) + r"_task-([^_]+)_run-"
        )

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
        """
        Build expected files:
          maskinfo:           fixed list (80 files, hemi-specific ROI names)
          events-{task}:      discovered per task, expects 3 runs
          bold-{task}:        discovered per task, expects 3 runs × 2 hemis
        """
        # sub and ses are going with session_dir, if session_dir is func/ then parent is ses and grandparent is sub
        sub = session_dir.parent.parent.name
        ses = session_dir.parent.name
        prefix = f"{sub}_{ses}"

        groups: dict[str, list[str]] = {}

        # --- Maskinfo group (always fixed) ---
        maskinfo_files = []
        for suffix in self.MASKINFO_SUFFIXES_HEMI_L:
            maskinfo_files.append(f"{prefix}{suffix}")
        for suffix in self.MASKINFO_SUFFIXES_HEMI_R:
            maskinfo_files.append(f"{prefix}{suffix}")
        groups["maskinfo"] = maskinfo_files

        # --- Discover tasks from files on disk ---
        event_tasks, bold_tasks = self._discover_tasks(session_dir, prefix)

        # --- Events: one group per discovered event task ---
        for task in sorted(event_tasks):
            expected = []
            for run in self.EXPECTED_RUNS:
                expected.append(f"{prefix}_task-{task}_run-{run}_events.tsv")
            groups[f"events-{task}"] = expected

        # --- Bold: one group per discovered bold task ---
        for task in sorted(bold_tasks):
            expected = []
            for run in self.EXPECTED_RUNS:
                for hemi in self.BOLD_HEMIS:
                    expected.append(
                        f"{prefix}_task-{task}_run-{run}_{hemi}_bold.nii.gz"
                    )
            groups[f"bold-{task}"] = expected

        return groups

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()


# =============================================================================
# 9. ANALYSIS SPEC: BIDS
# =============================================================================


class BIDSSpec(AnalysisSpec):
    """
    Raw BIDS: analysis_dir / sub-XX / ses-XX / {anat,func,fmap,dwi} / <files>
    Files grouped by modality subfolder, using glob patterns.
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
        self, analysis_dir: Path, sub: str, ses: str
    ) -> list[Path]:
        session = analysis_dir / sub / ses
        return [session / mod for mod in self.MODALITY_PATTERNS]

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        # sub and ses are going with session_dir, if session_dir is func/ then parent is ses and grandparent is sub
        sub = session_dir.parent.name
        ses = session_dir.name
        groups: dict[str, list[str]] = {}
        for modality, patterns in self.MODALITY_PATTERNS.items():
            expected = []
            for pat in patterns:
                resolved = pat.replace("{sub}", sub).replace("{ses}", ses)
                expected.append(f"{modality}/{resolved}")
            groups[modality] = expected
        return groups
    
class BIDSDWISpec(AnalysisSpec):
    """
    BIDS DWI check: analysis_dir / sub-XX / ses-XX / dwi / <files>

    Discovers acq- labels from files on disk, then for each acq expects:
      dir-AP_run-01:  .bval, .bvec, .json, .nii.gz
      dir-PA_run-01:  .json, .nii.gz
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
        self, analysis_dir: Path, sub: str, ses: str
    ) -> list[Path]:
        return [analysis_dir / sub / ses / "dwi"]

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        """
        Discover acq- labels from files, then build expected files per acq.
        session_dir = .../sub-XX/ses-XX/dwi/
        """
        import re

        if not session_dir.is_dir():
            return {}

        # sub/ses from path: dwi parent = ses, grandparent = sub
        sub = session_dir.parent.parent.name
        ses = session_dir.parent.name
        prefix = f"{sub}_{ses}"

        # Discover unique acq labels
        acq_pattern = re.compile(
            re.escape(prefix) + r"_acq-([^_]+)_dir-"
        )
        acq_labels: set[str] = set()
        for f in session_dir.iterdir():
            if f.is_file():
                m = acq_pattern.match(f.name)
                if m:
                    acq_labels.add(m.group(1))

        # Build expected files per acq
        groups: dict[str, list[str]] = {}
        for acq in sorted(acq_labels):
            expected = []
            # AP
            for ext in self.AP_EXTENSIONS:
                expected.append(
                    f"{prefix}_acq-{acq}_dir-AP_run-01_dwi{ext}"
                )
            # PA
            for ext in self.PA_EXTENSIONS:
                expected.append(
                    f"{prefix}_acq-{acq}_dir-PA_run-01_dwi{ext}"
                )
            groups[f"acq-{acq}"] = expected

        return groups

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()


# =============================================================================
# 10. ANALYSIS SPEC: fMRIPrep
# =============================================================================


class FMRIPrepSpec(AnalysisSpec):
    """fMRIPrep derivatives: analysis_dir / sub-XX / ses-XX / {anat,func,figures}"""

    MODALITY_PATTERNS: dict[str, list[str]] = {
        "anat": [
            "{sub}_{ses}_*desc-preproc_T1w.nii.gz",
            "{sub}_{ses}_*desc-brain_mask.nii.gz",
        ],
        "func": [
            "{sub}_{ses}_task-*_space-*_desc-preproc_bold.nii.gz",
            "{sub}_{ses}_task-*_desc-confounds_timeseries.tsv",
        ],
    }

    @property
    def name(self) -> str:
        return "fmriprep"

    @property
    def description(self) -> str:
        return "fMRIPrep preprocessed outputs"

    @property
    def uses_glob(self) -> bool:
        return True

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses

    def get_expected_subfolders(
        self, analysis_dir: Path, sub: str, ses: str
    ) -> list[Path]:
        session = analysis_dir / sub / ses
        return [session / mod for mod in self.MODALITY_PATTERNS] + [
            session / "figures"
        ]

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        # sub and ses are going with session_dir, if session_dir is func/ then parent is ses and grandparent is sub        
        sub = session_dir.parent.name
        ses = session_dir.name
        groups: dict[str, list[str]] = {}
        for modality, patterns in self.MODALITY_PATTERNS.items():
            expected = []
            for pat in patterns:
                resolved = pat.replace("{sub}", sub).replace("{ses}", ses)
                expected.append(f"{modality}/{resolved}")
            groups[modality] = expected
        return groups


# =============================================================================
# 11. ANALYSIS SPEC: GLM, RTP — placeholders
# =============================================================================


class GLMSpec(AnalysisSpec):
    """GLM analysis — customize EXPECTED_SUFFIXES for your pipeline."""
    EXPECTED_contrasts = [
        "AllvsNull",
        "PERvsNull",
        "LEXvsNull",
        "PERvsLEX",
        "RWvsLEX",
        "RWvsPER",
        "RWvsNull",
        "RWvsLEXPER",
        "RWvsAllnoWordnoLEX",
        "RWvsAllnoWord",
        "LEXvsAllnoLEXnoRW",
        "LEXvsAllnoLEXnoRWnoPER",
        "PERvsAllnoPERnoRW",
        "PERvsAllnoPERnoRWnoLEX",
        "CSvsFF",
        "FFvsCS",
        "FacesvsNull",
        "FacesvsLEX",
        "FacesvsPER",
        "FacesvsLEXPER",
        "FacesvsAllnoFace",
        "LimbsvsNull",
        "LimbsvsLEX",
        "LimbsvsPER",
        "LimbsvsLEXPER",
        "LimbsvsAllnoLimbs",
        "RWvsCS",
        "RWvsFF",
        "RWvsFace",
        "RWvsLimbs",
        "RWvsSD",
        "FacesvsLimbs"
    ]
    EXPECTED_SUFFIXES = [
        "stat-*.func.gii",  # placeholder — edit for your pipeline
    ]
    @property
    def name(self) -> str:
        return "glm"

    @property
    def description(self) -> str:
        return "GLM statistical analysis outputs"

    @property
    def uses_glob(self) -> bool:
        return True

    def get_session_dir(self, analysis_dir: Path, sub: str, ses: str) -> Path:
        return analysis_dir / sub / ses

    def get_expected_groups(self, session_dir: Path) -> dict[str, list[str]]:
        # sub and ses are going with session_dir, if session_dir is func/ then parent is ses and grandparent is sub        
        sub = session_dir.parent.name
        ses = session_dir.name
        return {"glm_outputs": [f"{sub}_{ses}_*{c}_{s}" for c in self.EXPECTED_contrasts for s in self.EXPECTED_SUFFIXES]}

    def get_default_combinations(self) -> list[tuple[str, str]]:
        return default_combinations()
class RTPSpec(AnalysisSpec):
    """RTP analysis — customize for your pipeline."""

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
        # sub and ses are going with session_dir, if session_dir is func/ then parent is ses and grandparent is sub        
        sub = session_dir.parent.name
        ses = session_dir.name
        return {"rtp_outputs": [f"{sub}_{ses}_{s}" for s in self.EXPECTED_SUFFIXES]}


# =============================================================================
# 12. SPEC REGISTRY
# =============================================================================

SPEC_REGISTRY: dict[str, AnalysisSpec] = {
    "prfprepare": PRFPrepareSpec(),
    "prfanalyze": PRFAnalyzeSpec(),
    "bids": BIDSSpec(),
    "bidsdwi": BIDSDWISpec(),
    "fmriprep": FMRIPrepSpec(),
    "glm": GLMSpec(),
    "rtp": RTPSpec(),
}


# =============================================================================
# 13. CLI
# =============================================================================


def parse_subses(raw: str) -> tuple[str, str]:
    """Parse '01,02' or 'sub-01,ses-02' into (sub_num, ses_num)."""
    parts = raw.strip().split(",")
    if len(parts) != 2:
        raise typer.BadParameter(f"Expected 'sub,ses' but got: '{raw}'")
    return parts[0].strip().replace("sub-", ""), parts[1].strip().replace("ses-", "")


def load_subseslist_from_file(filepath: Path) -> list[tuple[str, str]]:
    """Load sub/ses from CSV. Skips header/comment rows."""
    pairs = []
    with open(filepath) as f:
        for row in csv.reader(f):
            if not row or row[0].strip().lower() in ("sub", "subject", "#", ""):
                continue
            if len(row) >= 2:
                pairs.append((
                    row[0].strip().replace("sub-", ""),
                    row[1].strip().replace("ses-", ""),
                ))
    return pairs


@app.command()
def check(
    analysis_dir: Path = typer.Argument(
        ..., help="Path to analysis root directory.", exists=True,
    ),
    analysis_type: str = typer.Argument(
        ..., help=f"Analysis type: {', '.join(SPEC_REGISTRY.keys())}",
    ),
    subseslist: Optional[list[str]] = typer.Option(
        None, "--subseslist", "-s", help="Sub,ses pairs: -s 01,02 -s 03,04",
    ),
    subseslist_file: Optional[Path] = typer.Option(
        None, "--subseslist-file", "-f", help="CSV with sub,ses columns.",
    ),
    output_dir: Path = typer.Option(
        Path("."), "--output-dir", "-o", help="Directory for output reports.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show complete groups too.",
    ),
    show_distribution: bool = typer.Option(
        True, "--show-distribution/--no-distribution",
        help="Show group count distribution.",
    ),
) -> None:
    """Check analysis integrity against expected file/folder specs."""

    if analysis_type not in SPEC_REGISTRY:
        console.print(
            f"[red]Error:[/red] Unknown type '{analysis_type}'. "
            f"Valid: {list(SPEC_REGISTRY.keys())}"
        )
        raise typer.Exit(code=1)

    spec = SPEC_REGISTRY[analysis_type]

    # Resolve sub/ses list
    if subseslist:
        pairs = [parse_subses(s) for s in subseslist]
    elif subseslist_file:
        if not subseslist_file.exists():
            console.print(f"[red]Error:[/red] File not found: {subseslist_file}")
            raise typer.Exit(code=1)
        pairs = load_subseslist_from_file(subseslist_file)
    else:
        defaults = spec.get_default_combinations() #default_combinations()
        if defaults:
            pairs = [
                (s.replace("sub-", ""), ss.replace("ses-", ""))
                for s, ss in defaults
            ]
            console.print(
                f"[dim]Using default sub/ses from {spec.name} spec "
                f"({len(pairs)} combinations)[/dim]"
            )
        else:
            console.print(
                "[red]Error:[/red] Provide --subseslist or --subseslist-file"
            )
            raise typer.Exit(code=1)

    if not pairs:
        console.print("[red]Error:[/red] No sub/ses pairs.")
        raise typer.Exit(code=1)

    # Run
    console.print(
        f"\n[bold]{spec.name.upper()} integrity check[/bold] — {spec.description}"
    )
    console.print(f"Dir: {analysis_dir}")
    console.print(f"Sessions: {len(pairs)}\n")

    results = run_integrity_check(spec, analysis_dir, pairs)

    # Output
    print_summary(results, spec)
    if show_distribution:
        print_group_distribution(results)
    print_detailed_results(results, verbose)

    output_dir.mkdir(parents=True, exist_ok=True)
    brief_path = output_dir / f"{spec.name}_incomplete.csv"
    detail_path = output_dir / f"{spec.name}_detailed.log"
    write_brief_csv(results, brief_path)
    write_detailed_log(results, spec, detail_path)

    matrix_path = output_dir / f"{spec.name}_matrix.csv"
    write_matrix_csv(results, matrix_path)

    console.print(f"\n[bold]Brief CSV:[/bold]    {brief_path}")
    console.print(f"[bold]Detailed log:[/bold] {detail_path}")
    console.print(f"[bold]Matrix CSV:[/bold]   {matrix_path}")

if __name__ == "__main__":
    app()