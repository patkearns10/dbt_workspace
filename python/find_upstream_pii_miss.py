import json

with open("manifest.json") as f:
    manifest = json.load(f)

for node in manifest["nodes"].values():
    for col_name, col_meta in node.get("columns", {}).items():
        upstream_tags = set()
        for dep in col_meta.get("lineage", []):
            dep_tags = manifest["nodes"][dep]["columns"][col_name].get("tags", [])
            upstream_tags.update(dep_tags)
        if "pii" in upstream_tags and "pii" not in col_meta.get("tags", []):
            print(f"⚠️ {node['unique_id']} column {col_name} missing propagated tag: pii")
