"""
Test: can you remove a CIDR by omitting it from the PUT payload?

Hypothesis: the PUT endpoint treats the cidrs array as the desired final state
when CIDR objects include their existing `id`. Omitting an existing CIDR (by id)
should delete it; sending a new one without an id should add it.

Usage:
  export DBT_CLOUD_API_TOKEN="..."
  export DBT_ACCOUNT_ID="51798"
  export DBT_HOST="tk626.us1.dbt.com"
  export DBT_RULE_ID="1185"
  python3 test_cidr_remove.py
"""

import os, json, requests, copy

TOKEN      = os.environ["DBT_CLOUD_API_TOKEN"]
ACCOUNT_ID = os.environ["DBT_ACCOUNT_ID"]
HOST       = os.environ.get("DBT_HOST", "cloud.getdbt.com")
RULE_ID    = os.environ.get("DBT_RULE_ID", "1185")
RULE_URL   = f"https://{HOST}/api/v3/accounts/{ACCOUNT_ID}/ip-restrictions/{RULE_ID}"

HEADERS = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}

def get_rule():
    r = requests.get(RULE_URL, headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]

def put_rule(rule):
    return requests.put(RULE_URL, headers=HEADERS, json=rule)

def show_cidrs(label, rule):
    cidrs = [(c["id"], c["cidr"]) for c in rule.get("cidrs", [])]
    print(f"{label}: {cidrs}")

# ── 1. Snapshot ───────────────────────────────────────────────────────────────
rule = get_rule()
show_cidrs("Before", rule)
print(f"Full cidrs array:\n{json.dumps(rule['cidrs'], indent=2)}\n")

if len(rule["cidrs"]) < 2:
    print("Need at least 2 CIDRs on the rule to test removal. Add one first.")
    exit(1)

# ── 2. Build payload: keep all EXCEPT the last CIDR, add a new one ────────────
keep    = rule["cidrs"][:-1]          # existing CIDRs with their ids (all but last)
removed = rule["cidrs"][-1]           # the one we're dropping
new_cidr = {"cidr": "172.31.0.0/16"} # new CIDR without an id

payload = copy.deepcopy(rule)
payload["cidrs"] = keep + [new_cidr]

print(f"Removing : {removed['cidr']} (id={removed['id']})")
print(f"Adding   : {new_cidr['cidr']}")
print(f"Keeping  : {[c['cidr'] for c in keep]}")
print()

resp = put_rule(payload)
print(f"HTTP: {resp.status_code}")
body = resp.json()
print(json.dumps(body, indent=2))

if resp.ok:
    after = body["data"]
    show_cidrs("After ", after)
    after_cidrs = [c["cidr"] for c in after.get("cidrs", [])]
    removed_ok = removed["cidr"] not in after_cidrs
    added_ok   = new_cidr["cidr"] in after_cidrs
    print(f"\n{'✅' if removed_ok else '❌'} Removed {removed['cidr']}: {removed_ok}")
    print(f"{'✅' if added_ok   else '❌'} Added   {new_cidr['cidr']}: {added_ok}")