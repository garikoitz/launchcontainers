# Configuration file for the Sphinx documentation builder — MERGED
# Combines the existing conf.py (sphinx_rtd_theme, github_link, napoleon)
# with the new pydata_sphinx_theme + myst_parser setup.
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

# Pull version directly from the installed package (preserved from old config)
version = launchcontainers.__version__
release = launchcontainers.__version__

# ---------------------------------------------------------------------------
# General configuration
# ---------------------------------------------------------------------------
extensions = [
    # ---- Core API generation ------------------------------------------------
    "sphinx.ext.autodoc",  # pull docstrings automatically
    "sphinx.ext.autosummary",  # summary tables for modules/classes
    # ---- Docstring styles ---------------------------------------------------
    "sphinx.ext.napoleon",  # NumPy & Google style docstrings
    # ---- Cross-project links ------------------------------------------------
    "sphinx.ext.intersphinx",  # links to numpy, scipy, etc. docs
    # ---- Source links → GitHub ----------------------------------------------
    "sphinx.ext.linkcode",  # [source] button → GitHub line
    # ---- Plot/notebook support ----------------------------------------------
    "matplotlib.sphinxext.plot_directive",  # inline plots in docstrings
    "nbsphinx",  # render Jupyter notebooks
    # ---- Extras -------------------------------------------------------------
    "sphinx.ext.todo",  # .. todo:: directives
    "sphinx.ext.imgmath",  # math rendering
    # ---- Markdown support (new) ---------------------------------------------
    "myst_parser",  # parse .md files (README, CONTRIBUTING…)
]

autosummary_generate = True  # auto-generate stub .rst files
autosummary_imported_members = False  # don't expose re-imported names
add_module_names = False  # omit module prefix in names (old default)

autodoc_member_order = "bysource"  # preserve source order in API pages
autodoc_inherit_docstrings = True
autodoc_default_options = {
    "members": True,
    "undoc-members": False,  # skip members with no docstring
    "show-inheritance": True,
}

templates_path = ["../_templates"]

# Accept both .rst (existing pages) and .md (README, CONTRIBUTING, etc.)
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

master_doc = "index"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

pygments_style = "sphinx"

todo_include_todos = False

# Notebooks: never re-execute cells at build time
nbsphinx_execute = "never"

# ---------------------------------------------------------------------------
# Napoleon settings (fully preserved from old config)
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
# Intersphinx mapping (merged from both configs — old + new additions)
# ---------------------------------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/reference/", None),
    "matplotlib": ("https://matplotlib.org/stable/", None),  # updated URL
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "nibabel": ("https://nipy.org/nibabel/", None),  # from old config
    "nilearn": ("https://nilearn.github.io/stable/", None),  # updated URL
    "mne": ("https://mne.tools/stable/", None),  # from new config
}

# ---------------------------------------------------------------------------
# linkcode → GitHub (preserved from old config, URL corrected to garikoitz)
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
    # Graceful fallback if sphinxext/github_link.py is not present
    def linkcode_resolve(domain, info):
        """Fallback linkcode resolver when github_link is unavailable."""
        return None


# ---------------------------------------------------------------------------
# HTML output
# Switched from sphinx_rtd_theme → pydata_sphinx_theme for a modern look.
# If you ever need to revert, swap html_theme and html_theme_options below.
# ---------------------------------------------------------------------------
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "navigation_depth": 4,
    "show_nav_level": 1,
    "show_toc_level": 2,
    "show_prev_next": True,
    "collapse_navigation": False,
    "navbar_align": "left",
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/garikoitz/launchcontainers",
            "icon": "fa-brands fa-square-github",
            "type": "fontawesome",
        },
    ],
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links"],
    "footer_start": ["copyright"],
    "footer_end": ["sphinx-version"],
    "logo": {
        "text": "launchcontainers",
    },
}

html_title = "launchcontainers"
html_static_path = ["../_static"]
htmlhelp_basename = "launchcontainersdoc"  # preserved from old config

# The docs environment may not have the full scientific/runtime stack
# installed. Mocking these imports allows autodoc to import the package
# modules and render their function docstrings during ``make html``.
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
    """Register custom CSS/JS files.

    Notes
    -----
    Preserved from the original config.
    See https://github.com/rtfd/sphinx_rtd_theme/issues/117
    """
    app.add_css_file("theme_overrides.css")
    app.add_js_file("zenodo.js")
