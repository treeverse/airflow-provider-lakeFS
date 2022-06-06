<p align="center">
  <img src="https://raw.githubusercontent.com/treeverse/lakeFS/master/docs/assets/img/logo_large.png"/>
</p>

[![Apache license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://raw.githubusercontent.com/treeverse/lakeFS/master/LICENSE)
[![Provider test status](https://github.com/treeverse/airflow-provider-lakeFS/actions/workflows/provider.yaml/badge.svg)](https://github.com/treeverse/airflow-provider-lakeFS/actions/workflows/provider.yaml)
[![PyPI version](https://badge.fury.io/py/airflow-provider-lakefs.svg)](https://badge.fury.io/py/airflow-provider-lakefs)
[![Code of Conduct](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](https://github.com/treeverse/lakeFS/blob/master/CODE_OF_CONDUCT.md)


## lakeFS airflow provider

lakeFS airflow provider enables a smooth integration of lakeFS with airflow's DAGs.
"Use the lakeFS provider to create branches, commit objects, wait for files to be written, and more."

For usage example, check out the [example DAG](https://github.com/treeverse/airflow-provider-lakeFS/blob/main/lakefs_provider/example_dags/lakefs-dag.py)


## What is lakeFS

lakeFS is an open source layer that delivers resilience and manageability to object-storage based data lakes.

With lakeFS you can build repeatable, atomic and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS supports AWS S3, Azure Blob Storage and Google Cloud Storage as its underlying storage service. It is API compatible with S3, and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.


For more information see the [official lakeFS documentation](https://docs.lakefs.io).


## Capabilities

**Development Environment for Data**
* **Experimentation** - try tools, upgrade versions and evaluate code changes in isolation.
* **Reproducibility** - go back to any point of time to a consistent version of your data lake.

**Continuous Data Integration**
* **Ingest new data safely by enforcing best practices** - make sure new data sources adhere to your lake’s best practices such as format and schema enforcement, naming convention, etc.
* **Metadata validation** - prevent breaking changes from entering the production data environment.


**Continuous Data Deployment**
* **Instantly revert changes to data** - if low quality data is exposed to your consumers, you can revert instantly to a former, consistent and correct snapshot of your data lake.
* **Enforce cross collection consistency** - provide to consumers several collections of data that must be synchronized, in one atomic, revertible, action.
* **Prevent data quality issues by enabling**
    - Testing of production data before exposing it to users / consumers.
    - Testing of intermediate results in your DAG to avoid cascading quality issues.

## Community

Stay up to date and get lakeFS support via:

- [Slack](https://lakefs.io/slack) (to get help from our team and other users).
- [Twitter](https://twitter.com/lakeFS) (follow for updates and news)
- [YouTube](https://lakefs.io/youtube) (learn from video tutorials)
- [Contact us](https://lakefs.io/contact-us/) (for anything)

## More information

- [lakeFS documentation](https://docs.lakefs.io)
- If you would like to contribute, check out our [contributing guide](https://docs.lakefs.io/contributing).
- [Roadmap](https://docs.lakefs.io/roadmap.html)

## Licensing

lakeFS is completely free and open source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
