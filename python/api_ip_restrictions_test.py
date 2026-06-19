"""
Test script: dbt Cloud IP Restriction API — `extra` field behavior

Confirmed from DevTools + test runs:
  Method : PUT
  URL    : /api/v3/accounts/{account_id}/ip-restrictions/{rule_id}  (no trailing slash)
  Payload: full rule object, with `cidrs` as array of CIDR objects (can omit `id` for new ones)
  extra  : {"failed_cidrs": [...], "cidrs_already_exists": [...]}

Behavioral notes:
  - The API is ADDITIVE: sending a CIDR without an existing `id` appends it; it does NOT replace the rule's CIDRs.
  - failed_cidrs        : CIDRs that failed format validation (never saved)
  - cidrs_already_exists: valid CIDRs that are already on the rule (not saved again, not an error)
  - If at least one new valid CIDR is saved → 200 with extra showing partial failures
  - If ALL CIDRs are invalid             → 400 "Invalid IP range"
  - If ALL CIDRs already exist           → 409 "CIDR(s) already exists"

Usage:
  export DBT_CLOUD_API_TOKEN="..."
  export DBT_ACCOUNT_ID="51798"
  export DBT_HOST="tk626.us1.dbt.com"
  export DBT_RULE_ID="1185"
  python3 test_ip_restriction_extra.py
"""

import os, json, requests, copy

TOKEN      = os.environ["DBT_CLOUD_API_TOKEN"]
ACCOUNT_ID = os.environ["DBT_ACCOUNT_ID"]
HOST       = os.environ.get("DBT_HOST", "cloud.getdbt.com")
RULE_ID    = os.environ.get("DBT_RULE_ID", "1185")

RULE_URL   = f"https://{HOST}/api/v3/accounts/{ACCOUNT_ID}/ip-restrictions/{RULE_ID}"

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type":  "application/json",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_rule() -> dict:
    resp = requests.get(RULE_URL, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["data"]


def put_rule(rule: dict) -> requests.Response:
    """PUT the full rule object back (the shape the API expects)."""
    return requests.put(RULE_URL, headers=HEADERS, json=rule)


def make_rule_with_cidrs(base_rule: dict, cidr_strings: list[str]) -> dict:
    """
    Build a PUT payload from the base rule, replacing cidrs with the given strings.
    New CIDRs are added as minimal objects (no id); existing ones are preserved as-is
    to avoid orphaning them.
    """
    rule = copy.deepcopy(base_rule)
    rule["cidrs"] = [{"cidr": c} for c in cidr_strings]
    return rule


def print_result(label: str, description: str, cidrs: list[str], resp: requests.Response):
    print(f"\n{'='*60}")
    print(f"Test : {label}")
    print(f"       {description}")
    print(f"CIDRs: {cidrs}")
    print(f"HTTP : {resp.status_code}")

    try:
        body = resp.json()
    except ValueError:
        ct = resp.headers.get("Content-Type", "")
        print(f"Body : {'(HTML — bad payload or deleted rule)' if 'html' in ct else repr(resp.text[:300])}")
        print("\n❌  Non-JSON response.")
        return

    print(f"Body :\n{json.dumps(body, indent=2)}")

    if not resp.ok:
        print("\n❌  Request failed entirely.")
        return

    extra = body.get("extra", {})
    failed          = extra.get("failed_cidrs", [])
    already_exists  = extra.get("cidrs_already_exists", [])

    if failed or already_exists:
        print(f"\n⚠️  Partial result in `extra`:")
        if failed:
            print(f"   failed_cidrs        : {failed}")
        if already_exists:
            print(f"   cidrs_already_exists: {already_exists}")
    else:
        print(f"\n✅  All saved — extra.failed_cidrs=[], extra.cidrs_already_exists=[]")

# ── Test cases ────────────────────────────────────────────────────────────────

def main():
    print(f"Rule URL: {RULE_URL}")

    # Snapshot current state
    print("\n── Fetching current rule ─────────────────────────────────")
    try:
        rule = get_rule()
        print(json.dumps(rule, indent=2))
    except Exception as e:
        print(f"❌ Could not fetch rule: {e}")
        return

    original_cidrs = [c["cidr"] for c in rule.get("cidrs", [])]
    print(f"\nOriginal CIDRs: {original_cidrs}")

    # ── Test 1: all valid CIDRs ──────────────────────────────────
    payload = make_rule_with_cidrs(rule, ["10.0.0.0/8", "192.168.1.0/24"])
    resp = put_rule(payload)
    print_result(
        "All valid CIDRs",
        "Expect 200, extra.failed_cidrs=[]",
        ["10.0.0.0/8", "192.168.1.0/24"],
        resp,
    )

    # ── Test 2: mix of valid and invalid ────────────────────────
    mixed = [
        "10.0.0.0/8",           # valid
        "999.999.999.999/32",   # invalid — bad octets
        "192.168.1.0/24",       # valid
        "10.0.0.0/33",          # invalid — prefix > 32
        "2001:db8::/32",        # IPv6
    ]
    payload = make_rule_with_cidrs(rule, mixed)
    resp = put_rule(payload)
    print_result(
        "Mix of valid and invalid CIDRs",
        "Expect 200, extra.failed_cidrs=['999.999.999.999/32', '10.0.0.0/33']",
        mixed,
        resp,
    )

    # ── Test 3: mix with a CIDR already on the rule ─────────────
    # After Test 1, the rule already has 10.0.0.0/8 and 192.168.1.0/24.
    # Sending them again alongside a new valid CIDR should populate cidrs_already_exists.
    refreshed_rule = get_rule()
    existing_on_rule = {c["cidr"] for c in refreshed_rule.get("cidrs", [])}
    # Pick one that already exists and one genuinely new
    already_there = next(iter(existing_on_rule))
    mixed_dupes = [already_there, "172.16.0.0/12", "bad-cidr-here"]
    payload = make_rule_with_cidrs(refreshed_rule, mixed_dupes)
    resp = put_rule(payload)
    print_result(
        "Mix: existing CIDR + new valid + invalid",
        f"Expect 200: {already_there} → cidrs_already_exists, bad-cidr-here → failed_cidrs, 172.16.0.0/12 → saved",
        mixed_dupes,
        resp,
    )

    # ── Test 4: all invalid CIDRs ────────────────────────────────
    all_bad = ["not-a-cidr", "999.0.0.0/8", "10.0.0.0/99"]
    refreshed_rule = get_rule()
    payload = make_rule_with_cidrs(refreshed_rule, all_bad)
    resp = put_rule(payload)
    print_result(
        "All invalid CIDRs",
        "Expect 400 — nothing saved",
        all_bad,
        resp,
    )

    # ── Test 5: all duplicates (already on rule) ─────────────────
    refreshed_rule = get_rule()
    all_existing = [c["cidr"] for c in refreshed_rule.get("cidrs", [])]
    payload = make_rule_with_cidrs(refreshed_rule, all_existing[:2])
    resp = put_rule(payload)
    print_result(
        "All CIDRs already exist on rule",
        "Expect 409 — nothing new to save",
        all_existing[:2],
        resp,
    )

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("SUMMARY — extra field behavior:")
    print("  200 + extra.failed_cidrs populated    → at least one valid new CIDR saved, some invalid")
    print("  200 + extra.cidrs_already_exists      → some CIDRs already on rule (valid but skipped)")
    print("  400 'Invalid IP range'                → ALL submitted CIDRs failed validation")
    print("  409 'CIDR(s) already exists'          → ALL submitted CIDRs already exist on the rule")
    print(f"\nRule {RULE_ID} now has CIDRs: {[c['cidr'] for c in get_rule().get('cidrs', [])]}")
    print("(Clean up manually in the dbt platform UI if needed)")


if __name__ == "__main__":
    main()