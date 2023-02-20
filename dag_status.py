import time
import requests

def get_latest_state():
    url = "http://localhost:8080/api/v1/dags/lakeFS_workflow/dagRuns"
    username="admin"
    password="admin"
    response = requests.get( url,  auth=(username, password))
    dag_details = {}
    for key in response.json()['dag_runs']:
        dag_details[key['execution_date']] = key['state']

    # Creates a sorted dictionary (sorted by key)
    from collections import OrderedDict

    dict1 = OrderedDict(sorted(dag_details.items(), reverse=True))
    state = dict1[list(dict1.keys())[0]]
    return state

def dag_state():
    print("Inside the dag state block")

    state=get_latest_state()
    timeout = time.time() + 60 * 5  # 5 minutes from now
    while ((state != 'success') and (state != 'failed') and (state != 'skipped')):
        time.sleep(5)
        if time.time() > timeout:
            return 1
        state=get_latest_state()
        print("Dag details for LakeFS  workflow",state)
        continue
    if  state=='success':
            return 0
    else:
            return 1

print(dag_state())

