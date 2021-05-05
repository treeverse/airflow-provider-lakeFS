from airflow.decorators import dag

from lakefs_provider.operators.create_branch_operator import CreateBranchOperator
from lakefs_provider.sensors.commit_sensor import CommitSensor


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'lakeFS',
}


@dag(default_args=default_args, schedule_interval=None, tags=['example'])
def lakeFS_workflow():
    """
    ### Sample DAG

    Showcases the sample provider package's operator and sensor.

    To run this example, create a connector with:
    - id: conn_sample
    - type: http
    - host: www.httpbin.org	
    """

    task_create_branch = CreateBranchOperator(
        lakefs_conn_id="conn_1",
        repo="repo1",
        branch="airflow-created-branch",
        source_branch="main"
    )

    task_sense_commit = CommitSensor(
        lakefs_conn_id="conn_1",
        repo="repo1",
        branch="airflow-created-branch",
    )

    task_create_branch >> task_sense_commit


sample_workflow_dag = lakeFS_workflow()
