from typing import Dict
from typing import Sequence

from collections import namedtuple
from itertools import zip_longest
import time

from io import StringIO

from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

from lakefs_provider.hooks.lakefs_hook import LakeFSHook
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from lakefs_provider.operators.create_symlink_operator import LakeFSCreateSymlinkOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator
from lakefs_provider.operators.upload_operator import LakeFSUploadOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.get_commit_operator import LakeFSGetCommitOperator
from lakefs_provider.operators.get_object_operator import LakeFSGetObjectOperator
from lakefs_provider.sensors.file_sensor import LakeFSFileSensor
from lakefs_provider.sensors.commit_sensor import LakeFSCommitSensor
from airflow.operators.python import PythonOperator


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": "example-branch",
    "repo": "example-repo",
    "path": "path/to/_SUCCESS",
    "default-branch": "main",
    "lakefs_conn_id": "conn_lakefs"
}


CONTENT_PREFIX = 'It is not enough to succeed.  Others must fail.'
COMMIT_MESSAGE_1 = 'committing to lakeFS using airflow!'
MERGE_MESSAGE_1 = 'merging to the default branch'


IdAndMessage = namedtuple('IdAndMessage', ['id', 'message'])


def check_expected_prefix(task_instance, actual: str, expected: str) -> None:
    if not actual.startswith(expected):
        raise AirflowFailException(f'Got:\n"{actual}"\nwhich does not start with\n{expected}')


def check_logs(task_instance, repo: str, ref: str, commits: Sequence[str], messages: Sequence[str], amount: int=100) -> None:
    hook = LakeFSHook(default_args['lakefs_conn_id'])
    expected = [ IdAndMessage(commit, message) for commit, message in zip(commits, messages) ]
    actuals = (IdAndMessage(message=commit['message'], id=commit['id'])
               for commit in hook.log_commits(repo, ref, amount))
    for (expected, actual) in zip_longest(expected, actuals):
        if expected is None:
            # Matched all msgs!
            return
        if expected != actual:
            raise AirflowFailException(f'Got {actual} instead of {expected}')


class NamedStringIO(StringIO):
    def __init__(self, content: str, name: str) -> None:
        super().__init__(content)
        self.name = name


@dag(default_args=default_args,
     render_template_as_native_obj=True,
     max_active_runs=1,
     start_date=days_ago(2),
     schedule_interval=None,
     tags=['testing'])
def lakeFS_workflow():
    """
    ### Example lakeFS DAG

    Showcases the lakeFS provider package's operators and sensors.

    To run this example, create a connector with:
    - id: conn_lakefs
    - type: http
    - host: http://localhost:8000
    - extra: {"access_key_id":"AKIAIOSFODNN7EXAMPLE","secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
    """

    # Create the branch to run on
    task_create_branch = LakeFSCreateBranchOperator(
        task_id='create_branch',
        source_branch=default_args.get('default-branch')
    )

    # Create a path.
    task_create_file = LakeFSUploadOperator(
        task_id='upload_file',
        content=NamedStringIO(content=f"{CONTENT_PREFIX} @{time.asctime()}", name='content'))

    task_get_branch_commit = LakeFSGetCommitOperator(
        do_xcom_push=True,
        task_id='get_branch_commit',
        ref=default_args['branch'])

    # Checks periodically for the path.
    # DAG continues only when the file exists.
    task_sense_file = LakeFSFileSensor(
        task_id='sense_file',
        mode='reschedule',
        poke_interval=1,
        timeout=10,
    )

    # Commit the changes to the branch.
    # (Also a good place to validate the new changes before committing them)
    task_commit = LakeFSCommitOperator(
        task_id='commit',
        msg=COMMIT_MESSAGE_1,
        metadata={"committed_from": "airflow-operator"}
    )

    # Create symlink file for example-branch
    task_create_symlink = LakeFSCreateSymlinkOperator(task_id="create_symlink")

    # Wait until the commit is completed.
    # Not really necessary in this DAG, since the LakeFSCommitOperator won't return before that.
    # Nonetheless we added it to show the full capabilities.
    task_sense_commit = LakeFSCommitSensor(
        task_id='sense_commit',
        prev_commit_id='''{{ task_instance.xcom_pull(task_ids='get_branch_commit', key='return_value').id }}''',
        mode='reschedule',
        poke_interval=1,
        timeout=10,
    )

    # Get the file.
    task_get_file = LakeFSGetObjectOperator(
        task_id='get_object',
        do_xcom_push=True,
        ref=default_args['branch'])

    # Check its contents
    task_check_contents = PythonOperator(
        task_id='check_expected_prefix',
        python_callable=check_expected_prefix,
        op_kwargs={
            'actual': '''{{ task_instance.xcom_pull(task_ids='get_object', key='return_value') }}''',
            'expected': CONTENT_PREFIX,
        })

    # Merge the changes back to the main branch.
    task_merge = LakeFSMergeOperator(
        task_id='merge_branches',
        do_xcom_push=True,
        source_ref=default_args.get('branch'),
        destination_branch=default_args.get('default-branch'),
        msg=MERGE_MESSAGE_1,
        metadata={"committer": "airflow-operator"}
    )

    expectedCommits = ['''{{ ti.xcom_pull('merge_branches') }}''',
                       '''{{ ti.xcom_pull('commit') }}''']
    expectedMessages = [MERGE_MESSAGE_1, COMMIT_MESSAGE_1]

    # Fetch and verify log messages in bulk.
    task_check_logs_bulk = PythonOperator(
        task_id='check_logs_bulk',
        python_callable=check_logs,
        op_kwargs={
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='merge_branches', key='return_value') }}''',
            'commits': expectedCommits,
            'messages': expectedMessages,
        })

    # Fetch and verify log messages one at a time.
    task_check_logs_individually = PythonOperator(
        task_id='check_logs_individually',
        python_callable=check_logs,
        op_kwargs= {
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='merge_branches', key='return_value') }}''',
            'amount': 1,
            'commits': expectedCommits,
            'messages': expectedMessages,
        })


    task_create_branch >> task_get_branch_commit >> [task_create_file, task_sense_commit, task_sense_file]
    task_create_file >> task_commit >> task_create_symlink
    task_sense_file >> task_get_file >> task_check_contents
    task_sense_commit >> task_merge >> [task_check_logs_bulk, task_check_logs_individually]


sample_workflow_dag = lakeFS_workflow()
