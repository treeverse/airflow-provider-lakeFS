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
    latest = max(response.json()['dag_runs'], key=lambda k: k['execution_date'])
    state = latest['state']
    return state


def dag_state():
    state = get_latest_state()
    timeout = time.time() + 60 * 5  # 5 minutes from now
    while state != 'success' and state != 'failed' and state != 'skipped':
        time.sleep(5)
        if time.time() > timeout:
            return 1
        state = get_latest_state()
        continue
    if state == 'success':
        return 0
    else:
        return 1


sys.exit(dag_state())
