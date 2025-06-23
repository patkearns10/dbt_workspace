import os
import requests

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------

api_key = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
ACCOUNT_ID = 51798
CONNECTION_ID = 51284
DP_NAME = "Pat test project created from API"
DP_DESCRIPTION = "TBD"

response = requests.post(
    url = "https://cloud.getdbt.com/api/v3/accounts/51798/projects/",
    json = {
        "name": DP_NAME,
        "account_id": ACCOUNT_ID,
        "description": DP_DESCRIPTION,
        "connection_id": CONNECTION_ID,
        "state": 1,
        "type": 0,
    },
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    },
    verify=False
)

print(response.json())