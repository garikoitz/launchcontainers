# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024-2025
# All rights reserved.
#
# This script is distributed under the Apache-2.0 license.
#
# run_allses_glm.py
# -----------------
# Fit ONE GLM per subject by concatenating data across ALL sessions and their
# runs.  Contrast maps are written to:
#
#     <bids_dir>/derivatives/l1_surface/analysis-<output_name>/sub-<sub>/allses/
#
# Usage (single subject, explicit sessions)::
#
#     python run_allses_glm.py \
#         --base /scratch/tlei/VOTCLOC --sub 09 \
#         --sessions 01,02,03,04,05,06,07,08,09,10 \
#         --fp-ana-name 25.1.4_newest --task WCblock \
#         --space fsnative --start-scans 5 \
#         --contrast /path/to/contrast.yaml \
#         --output-name v2
#
# Usage (sessions from a subseslist file, sub column must be the same)::
#
#     python run_allses_glm.py \
#         --base /scratch/tlei/VOTCLOC -f wc_subseslist_23ses.txt --sub 09 \
#         ...
#
# The file form reads only rows where sub == --sub (and RUN==True if column
# present), collecting the distinct sessions for that subject.
# -----------------------------------------------------------------------------

from __future__ import annotations

import csv
import gc
import os
import os.path as op
import sys
import time
from os import makedirs
from typing import List, Optional

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
import pandas as pd
import typer
import yaml
from bids import BIDSLayout
from nilearn.plotting import plot_design_matrix
from nilearn.glm.first_level import make_first_level_design_matrix
from nilearn.glm.first_level.first_level import run_glm as nilearn_run_glm
from nilearn.glm.contrasts import compute_contrast
from nilearn.surface import load_surf_data
from rich import box
from rich.console import Console
from rich.table import Table
from scipy import stats

# Import shared helpers from run_glm.py by explicit file path.
# (A run_glm/ sub-directory in the same folder would shadow a normal import.)
import importlib.util as _ilu
_run_glm_path = op.join(op.dirname(op.abspath(__file__)), "run_glm", "run_glm.py")
_spec = _ilu.spec_from_file_location("_run_glm_mod", _run_glm_path)
_run_glm_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_run_glm_mod)

save_statmap_to_gifti     = _run_glm_mod.save_statmap_to_gifti
replace_prefix_and_suffix = _run_glm_mod.replace_prefix_and_suffix
load_contrasts            = _run_glm_mod.load_contrasts
_load_rerun_exclusions    = _run_glm_mod._load_rerun_exclusions
_print_timing_table       = _run_glm_mod._print_timing_table

console = Console()
app = typer.Typer(add_completion=False, pretty_exceptions_show_locals=False)


# ---------------------------------------------------------------------------
# Core: accumulate data across all sessions and runs
# ---------------------------------------------------------------------------

def prepare_allses_glm_input(
    bids_dir: str,
    fmriprep_dir: str,
    fp_layout: BIDSLayout,
    layout: BIDSLayout,
    label_dir: str,
    contrast_fpath: str,
    subject: str,
    sessions: list[str],
    task: str,
    start_scans: int,
    space: str,
    slice_time_ref: float,
    use_smoothed: bool,
    sm: str,
    apply_label_as_mask: str,
    rerun_excl: dict,
    hemi: str | None = None,
):
    """
    Load, z-score, and concatenate functional data across ALL sessions and
    their runs for *subject*, building a single design matrix ready for GLM.

    The onset times and frame_times for each (session, run) are shifted by the
    cumulative number of scans seen so far, so MATLAB sees a single continuous
    timeline.

    Returns
    -------
    tuple
        (conc_data_std, design_matrix_std, contrasts)
    """
    is_surface = space in ["fsnative", "fsaverage"]

    data_all: list[np.ndarray] = []
    frame_times_all: list[np.ndarray] = []
    events_all: list[pd.DataFrame] = []
    confounds_all: list[pd.DataFrame] = []

    total_scans_so_far = 0   # cumulative post-trim scans across sessions+runs
    t_r_global: float | None = None

    run_step_times: dict[str, dict[str, float]] = {}  # label → {step: seconds}

    for ses in sessions:
        excl_runs = rerun_excl.get((subject, ses, task), set())
        raw_runs = sorted(set(layout.get_runs(subject=subject, session=ses, task=task)))

        if not raw_runs:
            console.print(
                f"  [yellow]WARNING[/yellow]: no runs for sub-{subject} ses-{ses} "
                f"task-{task} — skipping session"
            )
            continue

        run_list = [f"{r:02d}" for r in raw_runs]
        if excl_runs:
            before   = set(run_list)
            run_list = [r for r in run_list if r not in excl_runs]
            excluded = sorted(before - set(run_list))
            if excluded:
                console.print(
                    f"  ses-{ses}: [yellow]excluded compensated runs:[/yellow] {excluded}"
                )

        console.print(f"\n  [bold]ses-{ses}[/bold]  runs: {run_list}")

        for run_num in run_list:
            run_label = f"ses-{ses}_run-{run_num}"
            run_step_times[run_label] = {}

            # ── Step 1: load functional data ─────────────────────────────────
            _t = time.time()
            query = {
                "subject":   subject,
                "session":   ses,
                "task":      task,
                "run":       run_num,
                "space":     space,
                "suffix":    "bold",
                "extension": ".func.gii" if is_surface else ".nii.gz",
            }
            if is_surface and hemi:
                query["hemi"] = hemi
            if use_smoothed:
                query["desc"] = f"smoothed{sm}"
            elif not is_surface:
                query["desc"] = "preproc"

            func_files = fp_layout.get(**query)
            if not func_files:
                console.print(
                    f"  [yellow]WARNING[/yellow]: no func file for {run_label} "
                    f"(query: {query}) — skipping"
                )
                continue

            func_file = func_files[0].path
            console.print(f"  Found: [dim]{func_file}[/dim]")

            if is_surface:
                data = load_surf_data(func_file)
                data_float = np.vstack(data[:, :]).astype(float)
            else:
                img = nib.load(func_file)
                arr = img.get_fdata()
                data_float = arr.reshape(-1, arr.shape[3]).astype(float)

            run_step_times[run_label]["load_func"] = time.time() - _t

            # ── Step 2: z-score + trim ────────────────────────────────────────
            _t = time.time()
            data_trimmed = data_float[:, start_scans:]
            data_std     = stats.zscore(data_trimmed, axis=1).astype(np.float32)
            del data_float, data_trimmed  # free raw data immediately
            n_features   = data_std.shape[0]

            if apply_label_as_mask and is_surface:
                from run_glm import load_surf_data as _lsd
                label_path = f"{label_dir}/{apply_label_as_mask}"
                surf_mask  = load_surf_data(label_path)
                mask_arr   = np.zeros((n_features, 1))
                mask_arr[surf_mask] = 1
                data_std = data_std * mask_arr

            n_scans = data_std.shape[1]
            data_all.append(data_std)
            run_step_times[run_label]["zscore_mask"] = time.time() - _t
            console.print(
                f"  Trimmed length: {n_scans}  "
                f"[dim](load+zscore: {run_step_times[run_label]['load_func'] + run_step_times[run_label]['zscore_mask']:.1f} s)[/dim]"
            )

            # ── Step 3: TR from BOLD sidecar JSON ────────────────────────────
            _t = time.time()
            json_files = fp_layout.get(
                subject=subject, session=ses, task=task, run=run_num,
                suffix="bold", extension=".json",
            )
            if not json_files:
                console.print(
                    f"  [yellow]WARNING[/yellow]: no sidecar JSON for {run_label} — skipping"
                )
                data_all.pop()
                continue
            import json as _json
            with open(json_files[0].path) as _jf:
                t_r = float(_json.load(_jf)["RepetitionTime"])
            if t_r_global is None:
                t_r_global = t_r
            run_step_times[run_label]["read_tr"] = time.time() - _t

            # ── Step 4: events.tsv from BIDS layout ───────────────────────────
            _t = time.time()
            ev_files = layout.get(
                subject=subject, session=ses, task=task, run=run_num,
                suffix="events", extension=".tsv",
            )
            if not ev_files:
                console.print(
                    f"  [yellow]WARNING[/yellow]: no events.tsv for {run_label} — skipping"
                )
                data_all.pop()
                continue
            events = pd.read_csv(ev_files[0].path, sep="\t")

            # ── Step 5: confounds from fMRIprep layout ────────────────────────
            conf_files = fp_layout.get(
                subject=subject, session=ses, task=task, run=run_num,
                desc="confounds", suffix="timeseries", extension=".tsv",
            )
            if not conf_files:
                console.print(
                    f"  [yellow]WARNING[/yellow]: no confounds TSV for {run_label} — skipping"
                )
                data_all.pop()
                continue
            confounds = pd.read_csv(conf_files[0].path, sep="\t")
            run_step_times[run_label]["read_events_confounds"] = time.time() - _t

            # ── Step 6: confounds + frame_times ──────────────────────────────
            _t = time.time()

            # Shift onset times by cumulative scan count
            events = events.copy()
            events.loc[:, "onset"] = events["onset"] + total_scans_so_far * t_r
            events_nobaseline = events[events["trial_type"] != "baseline"]
            events_all.append(events_nobaseline)

            # Frame times start at total_scans_so_far
            frame_times = t_r * (
                (np.arange(n_scans) + slice_time_ref) + total_scans_so_far
            )
            frame_times_all.append(frame_times)

            # Confounds
            motion_keys        = ["framewise_displacement", "rot_x", "rot_y", "rot_z", "trans_x", "trans_y", "trans_z"]
            a_compcor_keys     = [k for k in confounds.keys() if "a_comp_cor" in k]
            non_steady_keys    = [k for k in confounds.keys() if "non_steady"  in k]
            cosine_keys        = [k for k in confounds.keys() if "cosine"      in k]
            keep_keys          = motion_keys + a_compcor_keys + cosine_keys + non_steady_keys
            confounds_keep     = confounds[keep_keys].copy()
            confounds_keep["framewise_displacement"].iloc[0] = np.nanmean(
                confounds_keep["framewise_displacement"]
            )
            confounds_keep = confounds_keep.iloc[start_scans:]
            confounds_all.append(confounds_keep)

            total_scans_so_far += n_scans
            run_step_times[run_label]["confounds"] = time.time() - _t

            console.print(
                f"  Confounds length: {len(confounds_keep)}  "
                f"total_scans_so_far: {total_scans_so_far}  "
                f"[dim](confounds: {run_step_times[run_label]['confounds']:.1f} s)[/dim]"
            )

    if not data_all:
        raise RuntimeError(
            f"No data collected for sub-{subject} over sessions {sessions}."
        )

    # ── Per-run timing summary ────────────────────────────────────────────────
    step_cols = ["load_func", "zscore_mask", "read_tr", "read_events_confounds", "confounds"]
    tbl = Table(title="Per-run step timing (s)", box=box.SIMPLE_HEAD)
    tbl.add_column("ses_run")
    for s in step_cols:
        tbl.add_column(s, justify="right")
    tbl.add_column("total", justify="right")
    for rl, steps in run_step_times.items():
        vals = [steps.get(s, 0.0) for s in step_cols]
        tbl.add_row(rl, *[f"{v:.2f}" for v in vals], f"{sum(vals):.2f}")
    console.print(tbl)

    # ── Build concatenated design matrix ─────────────────────────────────────
    _t = time.time()
    conc_data_std      = np.concatenate(data_all, axis=1)
    del data_all        # free individual-run arrays now that concatenation is done
    concat_frame_times = np.concatenate(frame_times_all, axis=0)
    concat_events      = pd.concat(events_all, axis=0)
    concat_events      = concat_events.applymap(replace_prefix_and_suffix)
    concat_confounds   = pd.concat(confounds_all, axis=0)

    nonan_confounds = concat_confounds.dropna(axis=1, how="any")
    console.print(f"\n  Confound columns after dropna: {list(nonan_confounds.columns)}")

    design_matrix = make_first_level_design_matrix(
        concat_frame_times,
        events=concat_events,
        hrf_model="spm",
        drift_model=None,
        add_regs=nonan_confounds,
    )
    design_matrix_std = design_matrix.apply(stats.zscore, axis=0)
    design_matrix_std["constant"] = np.ones(len(design_matrix_std)).astype(int)

    contrasts = load_contrasts(contrast_fpath, design_matrix)
    console.print(
        f"  Design matrix: {conc_data_std.shape[1]} timepoints × {design_matrix_std.shape[1]} regressors  "
        f"[dim]({time.time() - _t:.2f} s)[/dim]"
    )

    return conc_data_std, design_matrix_std, contrasts


# ---------------------------------------------------------------------------
# GLM fit + output
# ---------------------------------------------------------------------------

def glm_allses(
    conc_data_std: np.ndarray,
    design_matrix_std: np.ndarray,
    contrasts: dict,
    bids_dir: str,
    task: str,
    space: str,
    subject: str,
    sessions: list[str],
    output_name: str,
    use_smoothed: bool = False,
    sm: str = "",
    hemi: str | None = None,
) -> dict[str, float]:
    """
    Fit the GLM and save contrast maps under
    ``<bids_dir>/derivatives/l1_surface/analysis-<output_name>/sub-<sub>/allses/``.

    Returns
    -------
    dict[str, float]
        Per-contrast wall-clock seconds.
    """
    ses_label = "allses"  # used in filename and output path

    outdir = op.join(
        bids_dir, "derivatives", "l1_surface",
        f"analysis-{output_name}", f"sub-{subject}", ses_label,
    )
    makedirs(outdir, exist_ok=True)

    console.print(f"[bold]------- GLM start  ({ses_label})[/bold]")

    plot_design_matrix(design_matrix_std)
    plt.savefig(op.join(outdir, "design_matrix.png"))
    plt.close()

    Y = np.transpose(conc_data_std)
    del conc_data_std   # Y is a view; free the name so GC can reclaim after GLM
    X = np.asarray(design_matrix_std)
    labels, estimates = nilearn_run_glm(Y, X, n_jobs=1)

    timing: dict[str, float] = {}

    for contrast_id, contrast_val in contrasts.items():
        t_c = time.time()

        if hemi:
            outname_base = (
                f"sub-{subject}_ses-{ses_label}_task-{task}"
                f"_hemi-{hemi}_space-{space}_contrast-{contrast_id}"
                f"_stat-X_statmap.func.gii"
            )
        else:
            outname_base = (
                f"sub-{subject}_ses-{ses_label}_task-{task}"
                f"_space-{space}_contrast-{contrast_id}"
                f"_stat-X_statmap.nii.gz"
            )
        if use_smoothed:
            outname_base = outname_base.replace("_statmap", f"_desc-smoothed{sm}_statmap")
        outname_base = op.join(outdir, outname_base)

        contrast_obj = compute_contrast(labels, estimates, contrast_val)
        betas    = contrast_obj.effect_size()
        t_value  = contrast_obj.stat()
        z_score  = contrast_obj.z_score()
        p_value  = contrast_obj.p_value()
        variance = contrast_obj.effect_variance()

        if hemi:
            save_statmap_to_gifti(betas,    outname_base.replace("stat-X", "stat-effect"))
            save_statmap_to_gifti(t_value,  outname_base.replace("stat-X", "stat-t"))
            save_statmap_to_gifti(z_score,  outname_base.replace("stat-X", "stat-z"))
            save_statmap_to_gifti(p_value,  outname_base.replace("stat-X", "stat-p"))
            save_statmap_to_gifti(variance, outname_base.replace("stat-X", "stat-variance"))
        else:
            console.print(
                f"  [yellow]WARNING[/yellow]: volumetric output not implemented, "
                f"skipping {outname_base}"
            )

        timing[contrast_id] = time.time() - t_c

    console.print(f"  [green]GLM done[/green]  (hemi-{hemi if hemi else 'volumetric'})")
    return timing


# ---------------------------------------------------------------------------
# Helpers: parse sessions
# ---------------------------------------------------------------------------

def _parse_subject_sessions(
    sub: str,
    sessions_arg: Optional[str],
    file_arg: Optional[str],
) -> list[str] | None:
    """
    Return an ordered list of zero-padded session strings for *sub*, or
    ``None`` when neither argument is given (caller will auto-detect from BIDS).

    Priority:
    1. ``--sessions 01,02,...`` explicit list
    2. ``-f <file>`` — read rows where sub column matches; respect RUN==True
    3. ``None`` — auto-detect from BIDS layout (handled in main)

    Raises typer.Exit on bad input.
    """
    if sessions_arg:
        return [s.strip().zfill(2) for s in sessions_arg.split(",") if s.strip()]

    if file_arg:
        from pathlib import Path
        path = Path(file_arg)
        if not path.exists():
            console.print(f"[red]ERROR[/red]: file not found: {file_arg}")
            raise typer.Exit(1)
        delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
        seen: dict[str, None] = {}  # ordered dedup
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh, delimiter=delimiter):
                row_sub = str(row["sub"]).strip().zfill(2)
                if row_sub != sub:
                    continue
                if "RUN" in row and str(row["RUN"]).strip() != "True":
                    continue
                ses = str(row["ses"]).strip().zfill(2)
                seen[ses] = None
        if not seen:
            console.print(
                f"[red]ERROR[/red]: no sessions found for sub-{sub} in {file_arg}"
            )
            raise typer.Exit(1)
        return list(seen)

    return None  # signal to auto-detect from BIDS


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.command()
def main(
    base: str = typer.Option(..., "--base", help="Base directory, e.g. /scratch/tlei/VOTCLOC"),
    sub: str = typer.Option(..., "--sub", help="Subject label without 'sub-', e.g. 09"),
    sessions_arg: Optional[str] = typer.Option(
        None, "--sessions",
        help="Comma-separated session labels, e.g. '01,02,03,04,05,06,07,08,09,10'",
    ),
    file_arg: Optional[str] = typer.Option(
        None, "-f",
        help="Subseslist CSV/TSV; rows for this --sub are used (RUN==True filter applied)",
    ),
    fp_ana_name: str = typer.Option(..., "--fp-ana-name", help="fMRIPrep analysis name"),
    task: str = typer.Option(..., "--task", help="Task name, e.g. WCblock"),
    start_scans: int = typer.Option(..., "--start-scans", help="Non-steady-state TRs to drop"),
    space: str = typer.Option(..., "--space", help="Space: fsnative | fsaverage | T1w | MNI152NLin2009cAsym"),
    contrast: str = typer.Option(..., "--contrast", help="Path to YAML contrast definition file"),
    output_name: str = typer.Option(..., "--output-name", help="Output folder label"),
    input_dirname: str = typer.Option("BIDS", "--input-dir", "-i", help="BIDS dir name under base"),
    slice_time_ref: float = typer.Option(0.5, "--slice-time-ref"),
    use_smoothed: bool = typer.Option(False, "--use-smoothed"),
    sm: str = typer.Option("", "--sm", help="FreeSurfer FWHM label, e.g. 05"),
    mask: str = typer.Option("", "--mask", help="FreeSurfer label file to apply as mask"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build design matrix but do not write outputs"),
    rerun_map: Optional[str] = typer.Option(
        None, "--rerun-map",
        help="Path to rerun_check.tsv for compensated-run exclusion",
    ),
) -> None:
    t0 = time.time()

    sub = sub.strip().zfill(2)
    sessions = _parse_subject_sessions(sub, sessions_arg, file_arg)

    bids_dir     = op.join(base, input_dirname)
    fsdir        = op.join(bids_dir, "derivatives", "freesurfer")
    fmriprep_dir = op.join(bids_dir, "derivatives", f"fmriprep-{fp_ana_name}")
    label_dir    = f"{fsdir}/sub-{sub}/label"
    is_surface   = space in ["fsnative", "fsaverage"]

    with open(contrast) as _f:
        n_contrasts = len(yaml.safe_load(_f))

    rerun_excl = _load_rerun_exclusions(rerun_map) if rerun_map else {}

    # Build layouts first so we can auto-detect sessions if needed
    console.print("Creating BIDS layout …")
    layout = BIDSLayout(bids_dir, validate=False)
    console.print("Creating fMRIPrep layout …")
    fp_layout = BIDSLayout(fmriprep_dir, validate=False)
    console.print("[green]Layouts ready.[/green]\n")

    # Auto-detect sessions from BIDS if neither --sessions nor -f was given
    if sessions is None:
        raw = sorted(s.zfill(2) for s in layout.get_sessions(subject=sub, task=task))
        if not raw:
            console.print(
                f"[red]ERROR[/red]: no sessions found in BIDS for "
                f"sub-{sub} task-{task}"
            )
            raise typer.Exit(1)

        # Filter: only keep sessions where fMRIprep has at least one func file
        # for this task (the func/ dir might exist but be empty or task-absent)
        ext = ".func.gii" if is_surface else ".nii.gz"
        sessions = []
        skipped  = []
        for s in raw:
            func_dir = op.join(fmriprep_dir, f"sub-{sub}", f"ses-{s}", "func")
            has_func = op.isdir(func_dir) and any(
                task in f and f.endswith(ext)
                for f in os.listdir(func_dir)
            )
            if has_func:
                sessions.append(s)
            else:
                skipped.append(s)

        if skipped:
            console.print(
                f"  [yellow]Skipped (no fMRIprep func for task-{task}):[/yellow] "
                f"{', '.join('ses-' + s for s in skipped)}"
            )
        if not sessions:
            console.print(
                f"[red]ERROR[/red]: no sessions with fMRIprep func data found "
                f"for sub-{sub} task-{task}"
            )
            raise typer.Exit(1)

        console.print(
            f"  [dim]Auto-detected {len(sessions)} valid session(s): "
            f"{', '.join('ses-' + s for s in sessions)}[/dim]\n"
        )

    # ── Launch summary ────────────────────────────────────────────────────────
    console.rule("[bold cyan]All-Sessions GLM Launch[/bold cyan]")
    tbl = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    tbl.add_column("key",   style="dim")
    tbl.add_column("value", style="bold")
    tbl.add_row("Subject",     f"sub-{sub}")
    tbl.add_row("Sessions",    f"({len(sessions)})  {', '.join('ses-' + s for s in sessions)}")
    tbl.add_row("Task",        task)
    tbl.add_row("Space",       space)
    tbl.add_row("fMRIPrep",    fp_ana_name)
    tbl.add_row("Output name", output_name)
    tbl.add_row("Start scans", str(start_scans))
    tbl.add_row("Contrasts",   f"{n_contrasts}  ({contrast})")
    tbl.add_row("Smoothed",    f"Yes (sm={sm})" if use_smoothed else "No")
    tbl.add_row("Mask",        mask or "—")
    tbl.add_row("Rerun map",   rerun_map or "— (no exclusions)")
    tbl.add_row("Mode",        "[yellow]DRY-RUN[/yellow]" if dry_run else "[green]EXECUTE[/green]")
    console.print(tbl)

    timing_per_hemi: dict[str, dict[str, float]] = {}
    hemis = ["L", "R"] if is_surface else [None]

    for hemi in hemis:
        label = f"hemi-{hemi}" if hemi else "volumetric"
        console.rule(f"[bold]Processing {label}[/bold]")

        conc_data_std, design_matrix_std, contrasts_dict = prepare_allses_glm_input(
            bids_dir=bids_dir,
            fmriprep_dir=fmriprep_dir,
            fp_layout=fp_layout,
            layout=layout,
            label_dir=label_dir,
            contrast_fpath=contrast,
            subject=sub,
            sessions=sessions,
            task=task,
            start_scans=start_scans,
            space=space,
            slice_time_ref=slice_time_ref,
            use_smoothed=use_smoothed,
            sm=sm,
            apply_label_as_mask=mask,
            rerun_excl=rerun_excl,
            hemi=hemi,
        )
        console.print(f"  Contrasts: {list(contrasts_dict.keys())}")

        if dry_run:
            console.print(
                "  [dim]Dry-run — design matrix and confounds printed above, "
                "nothing written.[/dim]"
            )
            del conc_data_std, design_matrix_std, contrasts_dict
            gc.collect()
            timing_per_hemi[label] = {}
            continue

        timing = glm_allses(
            conc_data_std=conc_data_std,
            design_matrix_std=design_matrix_std,
            contrasts=contrasts_dict,
            bids_dir=bids_dir,
            task=task,
            space=space,
            subject=sub,
            sessions=sessions,
            output_name=output_name,
            use_smoothed=use_smoothed,
            sm=sm,
            hemi=hemi,
        )
        # Free this hemisphere's data before loading the next one
        del conc_data_std, design_matrix_std, contrasts_dict
        gc.collect()   # return freed pages to OS before next hemi loads
        timing_per_hemi[label] = timing

    # ── Summary ───────────────────────────────────────────────────────────────
    total_elapsed = time.time() - t0
    console.rule("[bold cyan]Done[/bold cyan]")
    if any(v for v in timing_per_hemi.values()):
        _print_timing_table(timing_per_hemi, total_elapsed)
    else:
        console.print(
            f"  [dim]Dry-run completed.[/dim]  ({total_elapsed:.1f} s)"
        )
    console.print(
        f"  Output: [bold]{bids_dir}/derivatives/l1_surface/"
        f"analysis-{output_name}/sub-{sub}/allses/[/bold]"
    )


if __name__ == "__main__":
    app()
