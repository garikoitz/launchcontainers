![logo launchcontainers](https://user-images.githubusercontent.com/48440236/262432254-c7b53943-7c90-489c-933c-5f5a32510db4.png)
# launchcontainers

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Python tool for launching neuroimaging analysis pipelines on HPC clusters
with reproducible, BIDS-compliant workflows.

Developed at [BCBL – Basque Center on Cognition, Brain and Language](https://www.bcbl.eu).

---

## What it does

`launchcontainers` manages three phases of a neuroimaging analysis:

```
lc prepare  →  lc run  →  lc qc
```

**prepare** validates inputs, freezes your configs into the analysis directory,
and generates per-subject HPC launch scripts.
**run** submits those scripts to SLURM, SGE, or runs them locally.
**qc** checks outputs and writes a `failed_subseslist.tsv` for easy re-submission.

## Supported pipelines

| Type | Pipelines |
|---|---|
| DWI / structural (container-based) | `anatrois`, `freesurferator`, `rtppreproc`, `rtp-pipeline`, `rtp2-preproc`, `rtp2-pipeline` |
| fMRI (analysis-based) | `glm`, `prf` |

## Install

```bash
# 1. Install pipx and poetry (once per machine)
pip install pipx
pipx install poetry

# 2. Clone and set up the virtual environment
git clone https://github.com/your-org/launchcontainers.git
cd launchcontainers
poetry env use python3
poetry install

# 3. Activate the environment
poetry shell
```

## Quick start

```bash
lc copy_configs --output /path/to/basedir/code/
# edit lc_config.yaml and subseslist.tsv
lc prepare --lc_config lc_config.yaml --sub_ses_list subseslist.tsv --container_specific_config rtppreproc.json
lc run     --workdir /path/to/analysis-dir --run_lc
lc qc      --workdir /path/to/analysis-dir
```

## Documentation

Full documentation — config reference, CLI reference, step-by-step tutorial,
and the developer extension guide — lives in [`docs/`](docs/).

Build locally:

```bash
pip install sphinx sphinx-rtd-theme
cd docs && make html
open build/html/index.html
```

## License

MIT — see [LICENSE](LICENSE).

Copyright © 2020–2026 Garikoitz Lerma-Usabiaga
Copyright © 2020–2022 Mengxing Liu
Copyright © 2022–2023 Leandro Lecca
Copyright © 2022–2026 Yongning Lei
Copyright © 2023 David Linhardt
Copyright © 2023 Iñigo Tellaetxe
