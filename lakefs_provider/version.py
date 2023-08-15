"""Report package version"""

import sys

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata


def __get_version(package: str) -> str:
    try:
        return metadata.version(package)
    except metadata.PackageNotFoundError as e:
        print("No package metadata found")
        return None
    except Exception as e:
        print(f"Get version: {e}")
        return None


# Extract version using method #5 of https://packaging.python.org/en/latest/guides/single-sourcing-package-version/#single-sourcing-the-version
__version__ = __get_version('airflow-provider-lakefs')
