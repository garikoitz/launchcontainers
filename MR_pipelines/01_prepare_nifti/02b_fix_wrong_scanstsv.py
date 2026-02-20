#!/usr/bin/env python3
import re, shutil, typer
from pathlib import Path
from datetime import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

def run_num(fn):
    m = re.search(r'_run-(\d+)', fn)
    return int(m.group(1)) if m else -1

def modality(fn):
    return fn.split('/')[0] if '/' in fn else 'unknown'

def series_key(fn):
    task = re.search(r'_task-([^_]+)', fn)
    dirn = re.search(r'_dir-([^_]+)', fn)
    after = re.sub(r'_run-\d+', '_RUN_', fn)
    suf = re.search(r'_RUN_[_.]?(.*)', after)
    return '__'.join(filter(None, [
        task.group(1) if task else '',
        dirn.group(1) if dirn else '',
        suf.group(1) if suf else fn.rsplit('_',1)[-1],
    ]))

def fix_run(fn, old, new, mod):
    os_ = f"{old:d}" if mod == 'fmap' else f"{old:02d}"
    ns_ = f"{new:d}" if mod == 'fmap' else f"{new:02d}"
    return re.sub(rf'_run-{re.escape(os_)}(?=[_.])', f'_run-{ns_}', fn)

@app.command()
def main(
    input:   Path = typer.Option(..., "--input", "-i"),
    execute: bool = typer.Option(False, "--execute"),
):
    if not input.exists():
        console.print(f"[red]Not found: {input}[/red]"); raise typer.Exit(1)

    df = pd.read_csv(input, sep='\t')
    console.print(f"\n[cyan]Loaded {len(df)} rows from {input}[/cyan]")
    changes = []

    # ── Step 1: Dedup ─────────────────────────────────────────────────────────
    console.print("\n[yellow]── Step 1: Dedup (same randstr → keep higher run number) ──[/yellow]")
    t = Table(show_header=True, header_style="bold magenta")
    for col in ("Line", "Action", "Filename", "Run", "randstr"):
        t.add_column(col, justify="right" if col=="Line" else "left",
                     style="dim" if col=="Line" else None)

    keep_idx = []
    for rs, grp in df.groupby('randstr', sort=False):
        if len(grp) == 1:
            keep_idx.append(grp.index[0]); continue
        winner_idx = grp['filename'].apply(run_num).idxmax()
        for idx, row in grp.iterrows():
            line = idx + 2
            rn = run_num(row['filename'])
            ok = idx == winner_idx
            t.add_row(str(line),
                      "[green]KEEP[/green]" if ok else "[red]DROP[/red]",
                      row['filename'], str(rn) if rn >= 0 else '—', rs)
            if ok:
                keep_idx.append(idx)
            else:
                changes.append(('DROP', line, row['filename'], None))

    console.print(t)
    deduped = df.loc[keep_idx].drop_duplicates('filename').reset_index(drop=True)
    console.print(f"  {len(df)} → {len(deduped)} rows  (dropped {len(df)-len(deduped)})")

    # ── Step 2: Reindex ───────────────────────────────────────────────────────
    console.print("\n[yellow]── Step 2: Reindex runs by acq_time ──[/yellow]")
    deduped['_dt']  = pd.to_datetime(deduped['acq_time'])
    deduped['_mod'] = deduped['filename'].apply(modality)
    deduped['_key'] = deduped['filename'].apply(series_key)

    t2 = Table(show_header=True, header_style="bold magenta")
    for col in ("Row", "Old filename", "New filename", "Run"):
        t2.add_column(col, justify="right" if col=="Row" else "left",
                      style="dim" if col=="Row" else None)
    any_rename = False

    for (mod, key), grp in deduped.groupby(['_mod','_key'], sort=False):
        rows = grp[grp['filename'].str.contains(r'_run-\d+', regex=True)].sort_values('_dt')
        if rows.empty: continue

        if mod == 'func':
            times = rows['_dt'].tolist()
            slot, slots = 1, [1]
            for i in range(1, len(times)):
                if (times[i] - times[i-1]).total_seconds() > 30:
                    slot += 1
                slots.append(slot)
            expected = dict(zip(rows.index, slots))
        else:
            expected = {idx: i for i, idx in enumerate(rows.index, 1)}

        for idx, row in rows.iterrows():
            old_fn = row['filename']
            actual = run_num(old_fn)
            exp = expected[idx]
            if actual == exp: continue
            new_fn = fix_run(old_fn, actual, exp, mod)
            row_no = deduped.index.get_loc(idx) + 2
            t2.add_row(str(row_no), old_fn, new_fn, f"{actual} → {exp}")
            deduped.at[idx, 'filename'] = new_fn
            changes.append(('RENAME', row_no, old_fn, new_fn))
            any_rename = True

    console.print(t2 if any_rename else "  All run numbers correct.")

    deduped = (deduped.drop(columns=['_dt','_mod','_key'])
                      .sort_values('acq_time').reset_index(drop=True))

    console.print(f"\n[cyan]Summary:[/cyan]  dropped={sum(1 for c in changes if c[0]=='DROP')}  "
                  f"renamed={sum(1 for c in changes if c[0]=='RENAME')}  final={len(deduped)} rows")

    if not changes:
        console.print("[green]Nothing to do.[/green]"); return

    if execute:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        bak = input.with_name(input.stem + f'_backup_{ts}.tsv')
        shutil.copy2(input, bak)
        console.print(f"[cyan]Backup → {bak}[/cyan]")
        deduped.to_csv(input, sep='\t', index=False)
        console.print(f"[green]✓ Written → {input}[/green]")
    else:
        prev = input.with_name(input.stem + '_preview.tsv')
        deduped.to_csv(prev, sep='\t', index=False)
        console.print(f"[yellow]DRY RUN.[/yellow] Preview → [cyan]{prev}[/cyan]  |  add --execute to apply.")

if __name__ == "__main__":
    app()