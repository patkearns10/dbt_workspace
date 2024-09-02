import os
import requests

ACCOUNT_ID = 51798    # hardcode your account_id
HOW_MANY_JOBS = 1000  # hardcode your estimated number of jobs. If you are not sure, guess then add an extra zero.

# Store your dbt Cloud API token securely in your workflow tool
API_KEY = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Token {API_KEY}",
}

OFFSETS = range(0,HOW_MANY_JOBS+100,100)

for OFFSET in OFFSETS:
    r = requests.get(
        url=f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/?offset={OFFSET}",  #iterate through job ids
        headers=headers,
    )

    # Get job configuration.
    payload = r.json()["data"]

    job_ids = []
    for item in payload:
        job_ids.append(item["id"])


    if job_ids != []:
        print('=======================')
        print(f"Starting at offset: {OFFSET}")
        print(f"job ids: {job_ids}")
        for job in job_ids:
            r = requests.get(
                url=f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{job}/",
                headers=headers,
            )

            # Get job configuration.
            payload = r.json()["data"]
            print(payload)

            #=== THIS IS THE UPDATE SECTION, CUSTOMIZE TO YOUR LIKING ===#
            # Example using the target name field
            
            # Get job target_name.
            target_name = payload["settings"]["target_name"]

            # If job's target_name is unset (None or empty string "").
            if target_name is None or len(target_name) < 1:
                payload["settings"]["target_name"] = "default"
            #=== THIS IS THE END OF THE UPDATE SECTION ===#
            
            r = requests.post(
                url=f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{job}/",
                headers=headers,
                json=payload,
            )

            print(f"updated job id: {job}")
            print('=======================')

print('Done!')
