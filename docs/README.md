# launchcontainers Sphinx Documentation

## Folder structure

```
docs/
├── Makefile                  ← build commands live here
├── requirements.txt          ← docs-only dependencies
└── source/
    ├── conf.py               ← Sphinx configuration
    ├── index.rst             ← table of contents / home page
    ├── _static/              ← custom CSS/images (empty for now)
    ├── _templates/           ← custom Jinja2 templates (empty for now)
    └── api.rst
```

## Setup

```bash
# From repo root
pip install -r docs/requirements.txt

# Also install launchcontainers itself so autodoc can import it
pip install -e .
```

## Build HTML

```bash
cd docs
make html
# Output: docs/_build/html/index.html
```

## Live preview (auto-reloads on save)

```bash
cd docs
make livehtml
# Opens http://localhost:8000 in your browser
```

## Clean build artefacts

```bash
cd docs
make clean
```

## Re-generate API stubs from source

Only needed if you switch back to an ``api/`` stub-per-module layout:

```bash
cd docs
make apidoc
```

## Check external links

```bash
cd docs
make linkcheck
```
