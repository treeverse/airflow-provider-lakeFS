# 0.43.1

* Fix typing for Python 3.7 and above

# 0.43.0

* Added hooks:
  - LakeFSHook.logCommits

# 0.42.0

* Added operators:
  - LakeFSGetCommitOperator
  - LakeFSUploadOperator

* Changed operators:
  - LakeFSCommitSensor now supports optional `prev_commit_id` argument.
