"""
Diagnostic: find the right method + path for dbt Cloud IP restriction writes.

Usage:
  export DBT_CLOUD_API_TOKEN="..."
  export DBT_ACCOUNT_ID="51798"
  export DBT_HOST="tk626.us1.dbt.com"
  python3 diag_ip_restrictions.py
"""
import os, json, requests

TOKEN      = os.environ["DBT_CLOUD_API_TOKEN"]
ACCOUNT_ID = os.environ["DBT_ACCOUNT_ID"]
HOST       = os.environ.get("DBT_HOST", "cloud.getdbt.com")
BASE       = f"https://{HOST}/api/v3/accounts/{ACCOUNT_ID}/ip-restrictions"

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type":  "application/json",
}

def probe(method, url, payload=None):
    resp = getattr(requests, method)(url, headers=HEADERS, json=payload)
    is_html = "text/html" in resp.headers.get("Content-Type", "")
    body = "(HTML — frontend SPA, route not in API)" if is_html else repr(resp.text[:300])
    print(f"  {method.upper():6} {url}")
    print(f"         → {resp.status_code}  {body}")
    return resp

simple = {"cidr": "10.0.0.0/8"}

print("── 1. Collection endpoint ────────────────────────────────────")
for m in ("get", "post", "put", "patch", "delete"):
    probe(m, f"{BASE}/", simple if m != "get" else None)

print("\n── 2. Individual rule endpoint (id=1, likely 404 not 405) ───")
for m in ("get", "post", "put", "patch", "delete"):
    probe(m, f"{BASE}/1/", simple if m != "get" else None)

print("\n── 3. Try without trailing slash ─────────────────────────────")
for m in ("post", "patch"):
    probe(m, f"{BASE}", simple)
    probe(m, f"{BASE}/1", simple)