"""Setup.py for the lakeFS Airflow provider package"""

from setuptools import setup
import re

with open("README.md", "r") as fh:
    long_description = fh.read()

def _get_version(fh):
    r = re.compile("__version__ *= *[\"'](.*)[\"'] *")
    for line in fh.readlines():
        m = r.match(line)
        if m:
            return m.group(1)
    raise RuntimeError(f"No lines in {fh.name} define __version__")

with open("lakefs_provider/__init__.py", "r") as fh:
    version = _get_version(fh)

"""Perform the package airflow-provider-lakeFS setup."""
setup(
    name='airflow-provider-lakefs',
    version=version,
    description='A lakeFS provider package built by Treeverse.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=lakefs_provider.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['lakefs_provider', 'lakefs_provider.hooks', 'lakefs_provider.links',
              'lakefs_provider.sensors', 'lakefs_provider.operators',
              'lakefs_provider.example_dags'],
    install_requires=['apache-airflow>=2.0', 'lakefs_sdk>=0.113.0.2'],
    setup_requires=['setuptools', 'wheel'],
    author='Treeverse',
    author_email='services@treeverse.io',
    url='https://lakefs.io',
    python_requires='>=3.7',
)
