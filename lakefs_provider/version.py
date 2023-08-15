"""Report package version"""

import sys

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

# Extract version using method #5 of https://packaging.python.org/en/latest/guides/single-sourcing-package-version/#single-sourcing-the-version
__version__ = metadata.version('airflow-provider-lakefs')
