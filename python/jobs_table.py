"""
Install tabulate (for pretty printing):
# bash
pip install tabulate

You can set your environment variables like this before running:
# bash
export DBT_API_KEY=your_token_here
export ACCOUNT_ID=12345
export ENVIRONMENT_ID_ORIGINAL=370354
export ENVIRONMENT_ID_NEW=222

Run:
python api_get_job_intersection.py

Returns:
  ACCOUNT_ID  JOB_NAME         ENVIRONMENT_ID_ORIGINAL    JOB_ID_ORIGINAL    ENVIRONMENT_ID_NEW    JOB_ID_NEW
------------  -------------  -------------------------  -----------------  --------------------  ------------
       51798  Full Refresh                      370354             882109                 85030        106479
       51798  Nightly Build                     370354             882108                 85030         83771
"""

from datetime import datetime
import os
from pprint import pprint
from tabulate import tabulate
import csv
import json
import sys

# extract the current filepath
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
now = datetime.now().strftime('%Y.%m.%d.%H.%M.%S')
multiline_comment = "'''"

def main():
    with open(os.path.join(DIR_PATH, 'jobs.json')) as jobs:
        jobs_json = json.load(jobs)
        rows = []
        for job in jobs_json['data']:
            rows.append([
                job['account_id'],
                job['project_id'],
                job['environment_id'],
                job['id'],
                job['name']
            ])

        if rows:
            print(tabulate(rows, headers=[
                "ACCOUNT_ID", "PROJECT_ID", "ENVIRONMENT_ID", "JOB_ID", "JOB_NAME"
                ]))
        else:
            print("No jobs")


if __name__ == "__main__":
    main()
