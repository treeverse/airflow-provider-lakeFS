"""Setup.py for the lakeFS Airflow provider package"""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-lakeFS setup."""
setup(
    name='airflow-provider-lakefs',
    version='0.43.1',
    description='A lakeFS provider package built by Treeverse.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=lakefs_provider.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['lakefs_provider', 'lakefs_provider.hooks',
              'lakefs_provider.sensors', 'lakefs_provider.operators',
              'lakefs_provider.example_dags'],
    install_requires=['apache-airflow>=2.0', 'lakefs_client>=0.41.0.1'],
    setup_requires=['setuptools', 'wheel'],
    author='Treeverse',
    author_email='services@treeverse.io',
    url='https://lakefs.io',
    python_requires='>=3.6',
)
