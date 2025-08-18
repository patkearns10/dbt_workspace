import json
import os
import sys

FEATURE_FILE = "feature_dbt-bouncer_results.json"
DEV_FILE = "dev_dbt-bouncer_results.json"

def load_failures(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return {item["check_run_id"]: item["failure_message"] for item in data if item["outcome"] == "failed"}

def compare_failures():
    if not os.path.exists(FEATURE_FILE) or not os.path.exists(DEV_FILE):
        print(f"❌ Missing one or both of the input files: {FEATURE_FILE}, {DEV_FILE}")
        sys.exit(1)

    feature_failures = load_failures(FEATURE_FILE)
    dev_failures = load_failures(DEV_FILE)

    print(f"⚠️  Failures — feature: {len(feature_failures)} | dev: {len(dev_failures)}")

    new_failures = {k: v for k, v in feature_failures.items() if k not in dev_failures}

    fixed_failures = {k: v for k, v in dev_failures.items() if k not in feature_failures}

    if fixed_failures:
        print("\n✅ Failures fixed in feature branch:")
        for k, v in fixed_failures.items():
            print(f"  - {k}: {v}")

    if new_failures:
        print("\n❌ New failures introduced in feature branch:")
        for k, v in new_failures.items():
            print(f"  - {k}: {v}")
        print()
        raise RuntimeError(f"{len(new_failures)} new failure(s) introduced in feature.")

if __name__ == "__main__":
    compare_failures()
