#!/usr/bin/env python3
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
"""
Check phase-encoding direction of 3D-ME-EPI SWI DICOM series.

Sources tried in order:
  1. Siemens CSA series header (0029,1020) → PhaseEncodingDirectionPositive
  2. Siemens CSA image  header (0029,1010) → PhaseEncodingDirectionPositive
  3. dcm2niix JSON sidecar               → PhaseEncodingDirection (j / j-)
  4. Image orientation + InPlanePhaseEncodingDirection (geometric fallback)

Use --dump to print all DICOM tags and available CSA fields for one folder.

Usage:
    python check_pe_dir.py -s 05 -e swi -d /path/to/dcm_root
    python check_pe_dir.py -f subseslist.txt -d /path/to/dcm_root
    python check_pe_dir.py -s 05 -e swi -d /path/to/dcm_root --dump <folder_name>
"""

from __future__ import annotations

import json
import re
import struct
import subprocess
import tempfile
from pathlib import Path

import pydicom
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

# ---------------------------------------------------------------------------
# Shared constants (mirrors dcm2niix_swi.py)
# ---------------------------------------------------------------------------
SERIES_RE = re.compile(r"3D-ME-EPI", re.IGNORECASE)
SERIES_NUM_RE = re.compile(r"_(\d+)$")

ACQ_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"4TEs.*416slc", re.IGNORECASE), "4TE38"),
    (re.compile(r"5TEs.*352slc", re.IGNORECASE), "5TE35"),
]


def series_number(folder: Path) -> int:
    m = SERIES_NUM_RE.search(folder.name)
    return int(m.group(1)) if m else 0


def direction_from_name(folder: Path) -> str:
    up = folder.name.upper()
    return "PA" if ("IPE" in up or "RPE" in up) else "AP"


def first_dcm(folder: Path) -> Path | None:
    for ext in ("*.dcm", "*.DCM", "*.IMA", "*.ima"):
        files = sorted(f for f in folder.glob(ext) if f.is_file())
        if files:
            return files[0]
    # fallback: any file without extension
    files = sorted(f for f in folder.iterdir() if f.is_file() and "." not in f.name)
    return files[0] if files else None


# ---------------------------------------------------------------------------
# CSA header parser (manual — does not require nibabel)
# ---------------------------------------------------------------------------


def _parse_csa(raw: bytes) -> dict[str, str]:
    """Parse a Siemens CSA header (SV10 or NONAME format) into {name: value}."""
    result: dict[str, str] = {}
    try:
        if raw[:4] == b"SV10":
            # SV10 format
            n_tags = struct.unpack_from("<I", raw, 8)[0]
            pos = 16
            for _ in range(n_tags):
                name = raw[pos : pos + 64].split(b"\x00", 1)[0].decode("latin-1")
                pos += 64 + 4 + 4  # name + vm + vr
                n_items = struct.unpack_from("<I", raw, pos)[0]
                pos += 8  # n_items + xx
                values = []
                for _ in range(n_items):
                    item_len = struct.unpack_from("<I", raw, pos)[0]
                    pos += 16  # 4 lengths
                    val = (
                        raw[pos : pos + item_len]
                        .split(b"\x00", 1)[0]
                        .decode("latin-1")
                        .strip()
                    )
                    if val:
                        values.append(val)
                    pos += item_len + (4 - item_len % 4) % 4  # 4-byte aligned
                if values:
                    result[name] = values[0]
        else:
            # NONAME / older format
            n_tags = struct.unpack_from("<I", raw, 0)[0]
            pos = 8
            for _ in range(n_tags):
                name = raw[pos : pos + 64].split(b"\x00", 1)[0].decode("latin-1")
                pos += 84
                n_items = struct.unpack_from("<I", raw, pos)[0]
                pos += 16
                values = []
                for _ in range(n_items):
                    item_len = struct.unpack_from("<I", raw, pos)[0]
                    pos += 16
                    val = (
                        raw[pos : pos + item_len]
                        .split(b"\x00", 1)[0]
                        .decode("latin-1")
                        .strip()
                    )
                    if val:
                        values.append(val)
                    pos += item_len + (4 - item_len % 4) % 4
                if values:
                    result[name] = values[0]
    except Exception:
        pass
    return result


def read_csa_fields(ds: pydicom.Dataset) -> dict[str, str]:
    """Try all known CSA tag locations and return merged field dict."""
    fields: dict[str, str] = {}
    for tag in ((0x0029, 0x1020), (0x0029, 0x1010)):
        try:
            raw = bytes(ds[tag].value)
            fields.update(_parse_csa(raw))
        except (KeyError, Exception):
            pass
    return fields


# ---------------------------------------------------------------------------
# dcm2niix JSON fallback
# ---------------------------------------------------------------------------


def read_dcm2niix_json(folder: Path) -> dict:
    """Run dcm2niix -b o (JSON only) on folder and return parsed JSON."""
    try:
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(
                ["dcm2niix", "-b", "o", "-z", "n", "-f", "tmp", "-o", tmp, str(folder)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            jsons = list(Path(tmp).glob("*.json"))
            if jsons:
                return json.loads(jsons[0].read_text())
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Geometric fallback: derive AP/PA from image orientation
# ---------------------------------------------------------------------------


def direction_from_geometry(ds: pydicom.Dataset) -> str:
    """
    Derive phase-encoding direction polarity from image orientation cosines.
    Returns 'AP', 'PA', 'LR', 'RL', 'SI', 'IS', or '??' if undetermined.
    """
    try:
        iop = [float(v) for v in ds[0x0020, 0x0037].value]  # ImageOrientationPatient
        pe_axis = str(ds[0x0018, 0x1312].value)  # ROW or COL
    except KeyError:
        return "??"

    row_cos = iop[:3]  # direction cosine of image rows
    col_cos = iop[3:]  # direction cosine of image columns

    # Phase direction cosine
    pe_cos = col_cos if pe_axis == "COL" else row_cos

    # Largest component determines anatomical axis
    abs_cos = [abs(c) for c in pe_cos]
    dominant = abs_cos.index(max(abs_cos))
    sign = pe_cos[dominant]

    if dominant == 0:  # L-R axis
        return "LR" if sign > 0 else "RL"
    elif dominant == 1:  # A-P axis
        return "AP" if sign > 0 else "PA"
    else:  # S-I axis
        return "IS" if sign > 0 else "SI"


# ---------------------------------------------------------------------------
# Aggregate PE info from all sources
# ---------------------------------------------------------------------------


def read_pe_info(folder: Path) -> dict:
    result = {
        "inplane_tag": "n/a",
        "csa_pe_pos": "n/a",
        "csa_dir": "??",
        "json_pe_dir": "n/a",
        "json_dir": "??",
        "geom_dir": "??",
        "orient_text": "n/a",
        "final_dir": "??",
        "source": "none",
        "error": None,
    }

    dcm_file = first_dcm(folder)
    if dcm_file is None:
        result["error"] = "no DICOM file found"
        return result

    try:
        ds = pydicom.dcmread(str(dcm_file), stop_before_pixels=True)
    except Exception as e:
        result["error"] = str(e)
        return result

    # Standard axis tag
    try:
        result["inplane_tag"] = str(ds[0x0018, 0x1312].value)
    except KeyError:
        pass

    # Siemens orientation text (0051,100E)
    try:
        result["orient_text"] = str(ds[0x0051, 0x100E].value)
    except KeyError:
        pass

    # CSA header
    csa = read_csa_fields(ds)
    if "PhaseEncodingDirectionPositive" in csa:
        val = csa["PhaseEncodingDirectionPositive"]
        result["csa_pe_pos"] = val
        try:
            result["csa_dir"] = "PA" if int(val) == 1 else "AP"
            result["final_dir"] = result["csa_dir"]
            result["source"] = "CSA"
        except ValueError:
            pass

    # dcm2niix JSON fallback
    if result["final_dir"] == "??":
        jdata = read_dcm2niix_json(folder)
        pe = jdata.get("PhaseEncodingDirection", "")
        result["json_pe_dir"] = pe or "n/a"
        if pe in ("j", "j-", "i", "i-"):
            # j = PA (col direction positive), j- = AP
            mapping = {"j": "PA", "j-": "AP", "i": "RL", "i-": "LR"}
            result["json_dir"] = mapping.get(pe, "??")
            result["final_dir"] = result["json_dir"]
            result["source"] = "dcm2niix"

    # Geometric fallback
    geom = direction_from_geometry(ds)
    result["geom_dir"] = geom
    if result["final_dir"] == "??":
        result["final_dir"] = geom
        result["source"] = "geometry"

    return result


# ---------------------------------------------------------------------------
# Dump mode: print all DICOM tags + CSA fields for one folder
# ---------------------------------------------------------------------------


def dump_folder(folder: Path) -> None:
    dcm_file = first_dcm(folder)
    if dcm_file is None:
        console.print("[red]No DICOM file found[/red]")
        return

    console.print(f"[bold]DICOM file:[/bold] {dcm_file}")
    ds = pydicom.dcmread(str(dcm_file), stop_before_pixels=True)

    console.rule('All tags containing "phase" or "orient" (case-insensitive)')
    for elem in ds:
        kw = (elem.keyword or "").lower()
        if "phase" in kw or "orient" in kw:
            console.print(f"  ({elem.tag})  {elem.keyword:40s}  {elem.value}")

    console.rule("Private tags")
    for elem in ds:
        if elem.tag.group % 2 == 1:
            vlen = len(elem.value) if hasattr(elem.value, "__len__") else "scalar"
            console.print(f"  ({elem.tag})  VR={elem.VR}  len={vlen}")

    console.rule("CSA header fields")
    csa = read_csa_fields(ds)
    if csa:
        for k, v in sorted(csa.items()):
            if "phase" in k.lower() or "orient" in k.lower() or "encoding" in k.lower():
                console.print(f"  [cyan]{k}[/cyan] = {v}")
        console.print(f"  [dim](total CSA fields: {len(csa)})[/dim]")
        console.print("  [dim]Run with --dump-all-csa to see every field[/dim]")
    else:
        console.print(
            "  [yellow]No CSA fields parsed (tag may be absent or unreadable)[/yellow]"
        )
        console.print("  Tags tried: (0029,1020) and (0029,1010)")
        for tag in ((0x0029, 0x1020), (0x0029, 0x1010)):
            if tag in ds:
                raw = bytes(ds[tag].value)
                console.print(
                    f"  ({tag[0]:04X},{tag[1]:04X}) present, {len(raw)} bytes, "
                    f"first 8 bytes: {raw[:8]}"
                )
            else:
                console.print(f"  ({tag[0]:04X},{tag[1]:04X}) [red]not present[/red]")


# ---------------------------------------------------------------------------
# Per-subject check
# ---------------------------------------------------------------------------


def check_subject(sub: str, ses: str, dcm_dir: Path) -> None:
    swi_in = dcm_dir / f"sub-{sub}" / f"ses-{ses}"
    if not swi_in.exists():
        console.print(f"[red]Path not found:[/red] {swi_in}")
        return

    swi_dirs = sorted(
        [d for d in swi_in.iterdir() if d.is_dir() and SERIES_RE.search(d.name)],
        key=series_number,
    )
    if not swi_dirs:
        console.print(f"[yellow]No 3D-ME-EPI dirs in {swi_in}[/yellow]")
        return

    console.rule(f"sub-{sub}  ses-{ses}")

    table = Table(show_lines=True)
    table.add_column("#", style="dim", width=3)
    table.add_column("Series dir", style="cyan")
    table.add_column("Name\n→dir", width=6)
    table.add_column("(0018,1312)\naxis", width=6)
    table.add_column("CSA\nPE_pos", width=7)
    table.add_column("CSA\n→dir", width=6)
    table.add_column("JSON\nPE", width=5)
    table.add_column("Geom\n→dir", width=6)
    table.add_column("Final\ndir", width=6)
    table.add_column("Source", width=9)
    table.add_column("Match?", width=7)

    for idx, folder in enumerate(swi_dirs):
        name_dir = direction_from_name(folder)
        info = read_pe_info(folder)

        final = info["final_dir"]
        match = name_dir == final
        match_str = (
            "[green]YES[/green]"
            if match
            else ("[red]NO[/red]" if final != "??" else "[dim]--[/dim]")
        )

        table.add_row(
            str(idx),
            folder.name,
            name_dir,
            info["inplane_tag"],
            info["csa_pe_pos"],
            info["csa_dir"],
            info["json_pe_dir"],
            info["geom_dir"],
            f"[bold]{final}[/bold]",
            info["source"],
            match_str,
        )

        if info["error"]:
            console.print(f"  [red]Error {folder.name}: {info['error']}[/red]")

    console.print(table)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    sub: str = typer.Option(None, "-s", help="Subject ID (without sub- prefix)"),
    ses: str = typer.Option(None, "-e", help="Session ID (without ses- prefix)"),
    subseslist: Path = typer.Option(
        None, "-f", help="Path to subseslist CSV (skip header)"
    ),
    dcm_dir: Path = typer.Option(
        ..., "-d", help="DICOM root dir (sub-XX/ses-XX/series inside)"
    ),
    dump: str = typer.Option(
        None,
        "--dump",
        help='Dump all tags for this series folder name (e.g. "3D-ME-EPI_..._8")',
    ),
) -> None:
    """Check phase-encoding direction of 3D-ME-EPI SWI series from DICOM headers."""

    # Dump mode: inspect one specific folder
    if dump:
        if not sub or not ses:
            console.print("[red]--dump requires -s and -e[/red]")
            raise typer.Exit(1)
        target = dcm_dir / f"sub-{sub}" / f"ses-{ses}" / dump
        if not target.exists():
            # try prefix match
            parent = dcm_dir / f"sub-{sub}" / f"ses-{ses}"
            matches = [d for d in parent.iterdir() if d.name.startswith(dump)]
            if not matches:
                console.print(f"[red]Folder not found: {target}[/red]")
                raise typer.Exit(1)
            target = matches[0]
        dump_folder(target)
        return

    pairs: list[tuple[str, str]] = []
    if subseslist is not None:
        lines = subseslist.read_text().splitlines()
        for line in lines[1:]:
            parts = line.strip().split(",")
            if len(parts) >= 2 and parts[0] and parts[1]:
                pairs.append((parts[0].strip(), parts[1].strip()))
    elif sub and ses:
        pairs = [(sub, ses)]
    else:
        console.print("[red]Provide -s <sub> -e <ses>  or  -f <subseslist>[/red]")
        raise typer.Exit(1)

    for s, e in pairs:
        check_subject(s, e, dcm_dir)


if __name__ == "__main__":
    app()
