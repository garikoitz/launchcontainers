# Minimal Makefile for Sphinx documentation
# Run from the docs/ directory:  make html
# Or from the repo root:          make -C docs html

# Sphinx source and build directories (relative to this Makefile)
SOURCEDIR     = source
BUILDDIR      = _build

# Sphinx binary — use 'python -m sphinx' as fallback if sphinx-build not on PATH
SPHINXBUILD   ?= sphinx-build
SPHINXOPTS    ?= -W --keep-going   # -W turns warnings into errors; --keep-going doesn't stop on first error

.PHONY: help clean html livehtml linkcheck Makefile

## help  : Show this help message
help:
	@echo ""
	@echo "Usage:  make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^##' Makefile | sed 's/## /  /'
	@echo ""

## html  : Build HTML docs into docs/_build/html/
html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html
	@echo ""
	@echo "Build finished. Open $(BUILDDIR)/html/index.html to view."

## clean : Remove all build artefacts
clean:
	rm -rf $(BUILDDIR)
	@echo "Build directory cleaned."

## livehtml : Auto-rebuild and serve docs locally (requires sphinx-autobuild)
livehtml:
	sphinx-autobuild $(SOURCEDIR) $(BUILDDIR)/html \
		--open-browser \
		--port 8000 \
		--watch ../launchcontainers

## linkcheck : Check all external links in the docs
linkcheck:
	$(SPHINXBUILD) -b linkcheck $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/linkcheck
	@echo ""
	@echo "Link check done. Results in $(BUILDDIR)/linkcheck/output.txt"

## apidoc : Auto-generate API stubs from source code (run once to bootstrap)
apidoc:
	sphinx-apidoc -f -o $(SOURCEDIR)/api ../launchcontainers \
		--separate \
		--module-first \
		--no-toc
	@echo ""
	@echo "API stubs generated in $(SOURCEDIR)/api/"

# Catch-all: pass any unknown target to sphinx-build
%: Makefile
	$(SPHINXBUILD) -b $@ $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/$@
