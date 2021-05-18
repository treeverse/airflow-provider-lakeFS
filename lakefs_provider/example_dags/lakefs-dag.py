from typing import Dict

from airflow.decorators import dag
from airflow.utils.dates import days_ago

from lakefs_provider.operators.create_branch_operator import CreateBranchOperator
from lakefs_provider.operators.merge_operator import MergeOperator
from lakefs_provider.operators.commit_operator import CommitOperator
from lakefs_provider.sensors.file_sensor import FileSensor
from lakefs_provider.sensors.commit_sensor import CommitSensor


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": "example-branch",
    "repo": "example-repo",
    "default-branch": "main",
    "lakefs_conn_id": "conn_1"
}


@dag(default_args=default_args, start_date=days_ago(2), schedule_interval=None, tags=['example'])
def lakeFS_workflow():
    """
    ### Example lakeFS DAG

    Showcases the lakeFS provider package's operators and sensors.

    To run this example, create a connector with:
    - id: conn_1
    - type: http
    - host: http://localhost:8000
    - extra: {"access_key_id":"AKIAIOSFODNN7EXAMPLE","secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
    """

    # Create the branch to run on
    task_create_branch = CreateBranchOperator(
        task_id='create_branch',
        source_branch=default_args.get('default-branch')
    )

    # Checks periodically for the path.
    # DAG continues only when the file exists.
    task_sense_file = FileSensor(
        task_id='sense_file',
        path="file/to/sense/_SUCCESS"
    )

    # Commit the changes to the branch.
    # (Also a good place to validate the new changes before committing them)
    task_commit = CommitOperator(
        task_id='commit',
        msg="committing to lakeFS using airflow!",
        metadata={"committed_from": "airflow-operator"}
    )

    # Wait until the commit is completed.
    # Not really necessary in this DAG, since the CommitOperator won't return before that.
    # Nonetheless we added it to show the full capabilities.
    task_sense_commit = CommitSensor(
        task_id='sense_commit',
    )

    # Merge the changes back to the main branch.
    task_merge = MergeOperator(
        task_id='merge_branches',
        source_ref=default_args.get('branch'),
        destination_branch=default_args.get('default-branch'),
        msg='merging to the default branch',
        metadata={"committer": "airflow-operator"}
    )

    task_create_branch >> [task_sense_file, task_sense_commit]
    task_sense_file >> task_commit
    task_sense_commit >> task_merge


sample_workflow_dag = lakeFS_workflow()
