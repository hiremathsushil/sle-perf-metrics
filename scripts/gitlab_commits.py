#!/usr/bin/env python3

from datetime import datetime
from collections import Counter
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from socket import getfqdn

#Project_C = vt-perf-auto
#Project_D = SLEPerf
PROJECTS = {'Project_C': 5575, 'Project_D':6354}

BASE_API_URL = "https://gitlab.suse.de/api/v4"


def to_timestamp(source_date: str) -> float:
    "converts a date in format 2022-04-25 to unix nanosecond timestamp required by influxdb"
    return int(datetime.strptime(source_date, "%Y-%m-%d").timestamp()*1E9)

hostname = getfqdn()

session = requests.Session()
for pj_name, pj_id in PROJECTS.items():
    current_page = 0
    commits = Counter()
    while True:
        current_page += 1
        url = f"{BASE_API_URL}/projects/{pj_id}/repository/commits?page={current_page}"
        data = session.get(url, timeout=30, verify=False)
        for entry in data.json():
            day = entry['committed_date'][:10]
            commits[day] += 1
        # loop until header 'X-Next-Page' is empty
        if data.headers['X-Next-Page'] == '':
            break
    for date, value in commits.items():
        print(f"{pj_name},machine={hostname} commits={value} {to_timestamp(date)}")
