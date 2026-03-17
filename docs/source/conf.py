# Configuration file for the Sphinx documentation builder — MERGED
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.abspath("../.."))
sys.path.insert(0, os.path.abspath("../sphinxext"))

# ---------------------------------------------------------------------------
# Project information
# ---------------------------------------------------------------------------
import launchcontainers  # noqa: E402 (must come after sys.path setup)

project = "launchcontainers"
copyright = "2021-" + datetime.today().strftime("%Y") + ", launchcontainers Developers"
author = "Garikoitz Lerma-Usabiaga & launchcontainers Developers"

version = launchcontainers.__version__
release = launchcontainers.__version__

# ---------------------------------------------------------------------------
# General configuration
# ---------------------------------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.linkcode",
    "matplotlib.sphinxext.plot_directive",
    "nbsphinx",
    "sphinx.ext.todo",
    "sphinx.ext.imgmath",
    "myst_parser",
]

autosummary_generate = True
autosummary_imported_members = False
add_module_names = False

autodoc_member_order = "bysource"
autodoc_inherit_docstrings = True
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}

templates_path = ["../_templates"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

master_doc = "index"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Suppress warnings for intersphinx inventories that are temporarily
# unreachable (e.g. pandas docs server returning 5xx errors).
suppress_warnings = ["intersphinx.fetch_inventory"]

pygments_style = "sphinx"

todo_include_todos = False

nbsphinx_execute = "never"

# ---------------------------------------------------------------------------
# Napoleon settings
# ---------------------------------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = False
napoleon_use_ivar = True
napoleon_use_param = False
napoleon_use_keyword = True
napoleon_use_rtype = False

# ---------------------------------------------------------------------------
# Intersphinx mapping — URLs corrected March 2026
# ---------------------------------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
    "matplotlib": ("https://matplotlib.org/stable/", None),
    "nibabel": ("https://nipy.org/nibabel/", None),
    "nilearn": ("https://nilearn.github.io/stable/", None),
    "mne": ("https://mne.tools/stable/", None),
}

# ---------------------------------------------------------------------------
# linkcode → GitHub
# ---------------------------------------------------------------------------
try:
    from github_link import make_linkcode_resolve

    _linkcode_resolve = make_linkcode_resolve(
        "launchcontainers",
        "https://github.com/garikoitz/launchcontainers/blob/{revision}/{package}/{path}#L{lineno}",
    )

    def linkcode_resolve(domain, info):
        """Resolve documented Python objects to GitHub source links."""
        return _linkcode_resolve(domain, info)

except ImportError:

    def linkcode_resolve(domain, info):
        """Fallback linkcode resolver when github_link is unavailable."""
        return None


# ---------------------------------------------------------------------------
# HTML output — pydata_sphinx_theme
# ---------------------------------------------------------------------------
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "navigation_depth": 4,
    "show_nav_level": 2,
    "show_toc_level": 2,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/garikoitz/launchcontainers",
            "icon": "fa-brands fa-square-github",
            "type": "fontawesome",
        },
    ],
}

html_title = "launchcontainers"
html_static_path = ["../_static"]
htmlhelp_basename = "launchcontainersdoc"

autodoc_mock_imports = [
    "bids",
    "yaml",
    "pandas",
    "nibabel",
    "nilearn",
    "numpy",
    "scipy",
    "matplotlib",
    "dask",
    "dask_jobqueue",
    "distributed",
    "heudiconv",
    "typer",
    "rich",
    "requests",
    "openpyxl",
    "h5py",
]


def setup(app):
    """Register custom CSS/JS files."""
    app.add_css_file("theme_overrides.css")
    app.add_js_file("zenodo.js")
