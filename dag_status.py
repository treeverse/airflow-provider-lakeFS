#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import time
import requests


def get_latest_state():
    url = 'http://localhost:8080/api/v1/dags/lakeFS_workflow/dagRuns'
    username = 'admin'
    password = 'admin'
    response = requests.get(url, auth=(username, password))
    response.raise_for_status()
    dag_runs = response.json()['dag_runs']
    latest = max(dag_runs, key=lambda k: k['execution_date'])
    state = latest['state']
    return state


def dag_state():
    timeout = time.time() + 60 * 5  # five minutes from now
    interval = 5 # five seconds
    while True:
        state = get_latest_state()
        if state == 'success' or state == 'failed' or state == 'skipped':
            break
        if time.time()+interval > timeout:
            break
        time.sleep(interval)
    print("dag_state", state)
    if state != 'success':
        return 1
    return 0


sys.exit(dag_state())
