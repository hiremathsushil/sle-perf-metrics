#!/usr/bin/env python3

from datetime import datetime
from collections import Counter
import requests
import salt.client
from socket import getfqdn
# Initialize a Salt client
salt_client = salt.client.LocalClient()

# Retrieve Pillar data
pillar_data = salt_client.cmd('*', 'pillar.items', ['sle_config'])
github_token = pillar_data['127.0.0.1']['sle_config']['github_token']

PROJECTS = ['SUSE/qa-testsuites'] 

BASE_API_URL = "https://api.github.com/repos"

def to_timestamp(source_date: str) -> float:
    return int(datetime.strptime(source_date, "%Y-%m-%d").timestamp() * 1E9)

headers = {"Authorization": f"token {github_token}"}

session = requests.Session()
hostname = getfqdn()

for pj_name in PROJECTS:
    current_page = 1
    commits = Counter()
    while True:
        url = f"{BASE_API_URL}/{pj_name}/commits?page={current_page}&per_page=100"
        data = session.get(url, headers=headers, timeout=30)
        json_data = data.json()

        # Stop if we reach the end or hit an API issue
        if not json_data or 'message' in json_data:
            break

        for entry in json_data:
            day = entry['commit']['committer']['date'][:10]
            commits[day] += 1

        current_page += 1

    for date, value in commits.items():
        print(f"{pj_name.replace('/', '_')},machine={hostname}  commits={value} {to_timestamp(date)}")

