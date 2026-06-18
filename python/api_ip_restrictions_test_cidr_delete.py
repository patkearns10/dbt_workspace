"""
Test: remove a CIDR by setting state=2 in the PUT payload.

Discovered via DevTools: the UI sends the full cidrs array with state=2 on
CIDRs to delete. The backend removes them; they don't appear in the response.

  state: 1 = active
  state: 2 = delete this CIDR

Usage:
  export DBT_CLOUD_API_TOKEN="..."
  export DBT_ACCOUNT_ID="51798"
  export DBT_HOST="tk626.us1.dbt.com"
  export DBT_RULE_ID="1185"
  python3 test_cidr_state2_delete.py
"""

import os, json, requests, copy

TOKEN      = os.environ["DBT_CLOUD_API_TOKEN"]
ACCOUNT_ID = os.environ["DBT_ACCOUNT_ID"]
HOST       = os.environ.get("DBT_HOST", "cloud.getdbt.com")
RULE_ID    = os.environ.get("DBT_RULE_ID", "1185")

RULE_URL = f"https://{HOST}/api/v3/accounts/{ACCOUNT_ID}/ip-restrictions/{RULE_ID}"
HEADERS  = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}

def get_rule():
    r = requests.get(RULE_URL, headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]

def show_cidrs(label, rule):
    entries = [(c["id"], c["cidr"], c.get("state"), c.get("enabled")) for c in rule.get("cidrs", [])]
    print(f"{label}: {entries}")

# ── Snapshot ──────────────────────────────────────────────────────────────────
rule = get_rule()
show_cidrs("Before", rule)

if len(rule["cidrs"]) < 2:
    print("\nNeed at least 2 CIDRs to test removal. Add one first:")
    print("  Run test_ip_restriction_extra.py to add some CIDRs.")
    # Add a test CIDR first
    print("\nAdding 10.0.0.0/8 for testing...")
    resp = requests.put(RULE_URL, headers=HEADERS, json={
        **rule, "cidrs": [{"cidr": "10.0.0.0/8"}]
    })
    rule = resp.json()["data"]
    show_cidrs("After add", rule)

# ── Build payload: keep first CIDR, mark rest as state=2 (delete) ─────────────
target = rule["cidrs"][-1]   # the CIDR to remove
keep   = rule["cidrs"][:-1]  # everything else stays

payload = copy.deepcopy(rule)
payload["cidrs"] = keep + [{**target, "state": 2}]  # mark target for deletion

print(f"\nRemoving : {target['cidr']} (id={target['id']}) via state=2")
print(f"Keeping  : {[c['cidr'] for c in keep]}")

resp = requests.put(RULE_URL, headers=HEADERS, json=payload)
print(f"\nHTTP: {resp.status_code}")
body = resp.json()
print(json.dumps(body, indent=2))

# ── Verify ────────────────────────────────────────────────────────────────────
after = body.get("data", {})
show_cidrs("After ", after)
after_cidrs = [c["cidr"] for c in after.get("cidrs", [])]

removed = target["cidr"] not in after_cidrs
kept    = all(c["cidr"] in after_cidrs for c in keep)

print(f"\n{'✅' if removed else '❌'} {target['cidr']} removed: {removed}")
print(f"{'✅' if kept   else '❌'} Other CIDRs preserved: {kept}")

if removed and kept:
    print("\n✅ Confirmed: state=2 in the PUT payload hard-deletes a CIDR.")
    print("   Pattern: include the CIDR object with its id and state=2 to remove it.")