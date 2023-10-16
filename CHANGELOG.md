# CHANGELOG.md

## 0.48.0

  * Use new lakeFS Python SDK v0.113.0

## 0.46.2

  * Update lakeFS client SDK to v0.101.0.
    Airflow uses lakeFS merge API, you must be using lakeFS version 0.91.0 or
    higher because of breaking changes in response format. 

## 0.46.1

  * Actually add those missing Airflow "extra links" (#67), missing in
    0.46.0 :flushed:.  Without this you get buttons on lakeFS that take you
    to Airflow, but no "extra links" (buttons) on Airflow that take you to
    lakeFS.

## 0.46.0

  * Add Airflow DAG metadata to lakeFS commits and to merges (#47, #56, #57).

    Adds a clickable button (Airflow "extra link") to these tasks that takes
    you to the commit on lakeFS.  When used with a supporting lakeFS
    release, clickable buttons will appear on the commit.

## 0.45.0

  * Update lakefs_hook.py (#50)
  * Adding custom form connection (#49)
  * Verify workflow success in CI on Airflow (#48)
  * Use lakefs 0.91.0 (#46)
  * Feature: create symlink operator (#41)
  * Fix integration test to work on lakeFS/KV (#43)
  * Update docker-compose to align with key-value new configuration (#37)

## 0.44.0

  * Added operators:
    - LakeFSGetObjectOperator (Airflow limitation: small objects only)

## 0.43.1

  * Fix typing for Python 3.7 and above

## 0.43.0

  * Added hooks:
    - LakeFSHook.logCommits

## 0.42.0

  * Added operators:
    - LakeFSGetCommitOperator
    - LakeFSUploadOperator

  * Changed operators:
    - LakeFSCommitSensor now supports optional `prev_commit_id` argument.
