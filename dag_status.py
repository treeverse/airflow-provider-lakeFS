from datetime import datetime, date
from airflow.models.dagrun import DagRun
import time

def dag_state():
    print("Inside the dag state block")
    dag_id = 'lakeFS_workflow'
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: datetime.strptime(str(x.execution_date).split(".")[0], '%Y-%m-%d %H:%M:%S'),
                  reverse=True)
    print(dag_runs[0].execution_date,dag_runs[0].state)
    while ((dag_runs[0].state != 'success') and (dag_runs[0].state != 'failed') and (dag_runs[0].state != 'skipped')):
        time.sleep(5)
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: datetime.strptime(str(x.execution_date).split(".")[0], '%Y-%m-%d %H:%M:%S'),reverse=True)
        print("Dag details for LakeFS workflow",dag_runs[0].execution_date,dag_runs[0].state)
        continue
    print(dag_runs[0].state)
    if  dag_runs[0].state=='success':
            return 0
    else:
            return 1

dag_state()

