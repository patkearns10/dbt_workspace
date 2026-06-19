"""
Test DELETE /ip-restrictions/{rule_id} behavior across scenarios.

Scenarios:
  1. Successful delete of a real rule
  2. Delete of a non-existent rule ID
  3. Delete of an already-deleted rule (repeat call)
  4. Delete with an invalid token (auth failure)
  5. Delete with a valid token but wrong account ID

Usage:
  export DBT_CLOUD_API_TOKEN="..."
  export DBT_ACCOUNT_ID="51798"
  export DBT_HOST="tk626.us1.dbt.com"
  python3 test_ip_restriction_delete.py

NOTE: This script creates a throwaway rule, deletes it, then runs the remaining
scenarios against non-destructive targets (fake IDs, bad auth). Your existing
rules are not touched beyond the throwaway.
"""

import os, json, requests

TOKEN      = os.environ["DBT_CLOUD_API_TOKEN"]
ACCOUNT_ID = os.environ["DBT_ACCOUNT_ID"]
HOST       = os.environ.get("DBT_HOST", "cloud.getdbt.com")

BASE     = f"https://{HOST}/api/v3/accounts/{ACCOUNT_ID}"
HEADERS  = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}

FAKE_RULE_ID  = 999999999
FAKE_ACCOUNT  = 12345
BAD_TOKEN_HDR = {"Authorization": "Token totallyinvalidtoken123", "Content-Type": "application/json"}

SEP = "─" * 60

def show(label, resp):
    is_html = "text/html" in resp.headers.get("Content-Type", "")
    if is_html:
        body = "(HTML — SPA, not an API response)"
    elif resp.text.strip():
        try:
            body = json.dumps(resp.json(), indent=2)
        except Exception:
            body = resp.text[:300]
    else:
        body = "(empty body)"
    print(f"\n{SEP}")
    print(f"SCENARIO: {label}")
    print(f"HTTP {resp.status_code}")
    print(body)


# ── Setup: create a throwaway rule to safely delete ───────────────────────────

print("Creating throwaway rule for delete test...")
create_resp = requests.post(
    f"{BASE}/ip-restrictions/",
    headers=HEADERS,
    json={
        "name":        "delete-test-throwaway",
        "description": "Created by test script — safe to delete",
        "type":        1,
        "cidrs":       [{"cidr": "203.0.113.0/24"}],  # TEST-NET, documentation range
        "enabled":     True,
    },
)

if create_resp.status_code not in (200, 201):
    print(f"Could not create throwaway rule: {create_resp.status_code}")
    print(create_resp.text[:300])
    print("\nSkipping scenario 1 (successful delete) — running remaining scenarios only.")
    throwaway_id = None
else:
    throwaway_id = create_resp.json()["data"]["id"]
    print(f"Throwaway rule created: id={throwaway_id}")


# ── Scenario 1: Successful delete ────────────────────────────────────────────

if throwaway_id:
    resp = requests.delete(f"{BASE}/ip-restrictions/{throwaway_id}", headers=HEADERS)
    show("1 — Successful delete of a real rule", resp)
else:
    print(f"\n{SEP}")
    print("SCENARIO 1 — Skipped (rule creation failed)")


# ── Scenario 2: Delete a non-existent rule ID ─────────────────────────────────

resp = requests.delete(f"{BASE}/ip-restrictions/{FAKE_RULE_ID}", headers=HEADERS)
show(f"2 — Delete non-existent rule ID ({FAKE_RULE_ID})", resp)


# ── Scenario 3: Delete the already-deleted rule again (repeat call) ───────────

if throwaway_id:
    resp = requests.delete(f"{BASE}/ip-restrictions/{throwaway_id}", headers=HEADERS)
    show(f"3 — Repeat delete of already-deleted rule (id={throwaway_id})", resp)
else:
    print(f"\n{SEP}")
    print("SCENARIO 3 — Skipped (no throwaway rule was created)")


# ── Scenario 4: Invalid auth token ───────────────────────────────────────────

resp = requests.delete(f"{BASE}/ip-restrictions/{FAKE_RULE_ID}", headers=BAD_TOKEN_HDR)
show("4 — Invalid auth token", resp)


# ── Scenario 5: Valid token, wrong account ID ─────────────────────────────────

wrong_base = f"https://{HOST}/api/v3/accounts/{FAKE_ACCOUNT}"
resp = requests.delete(f"{wrong_base}/ip-restrictions/{FAKE_RULE_ID}", headers=HEADERS)
show(f"5 — Valid token, wrong account ID ({FAKE_ACCOUNT})", resp)


# ── Scenario 6: Trailing slash on the rule URL ───────────────────────────────

resp = requests.delete(f"{BASE}/ip-restrictions/{FAKE_RULE_ID}/", headers=HEADERS)
show(f"6 — Trailing slash on rule URL (id={FAKE_RULE_ID}/)", resp)


print(f"\n{SEP}")
print("Done.")