import json

with open("manifest.json") as f:
    manifest = json.load(f)

def get_column_tags(unique_id, col_name):
    """Get tags for a column from either nodes or sources"""
    if unique_id in manifest.get("nodes", {}):
        return manifest["nodes"][unique_id].get("columns", {}).get(col_name, {}).get("tags", [])
    elif unique_id in manifest.get("sources", {}):
        return manifest["sources"][unique_id].get("columns", {}).get(col_name, {}).get("tags", [])
    return []

# Debug: Check if source has PII tags
print("=== DEBUGGING SOURCES ===")
for source_id, source in manifest.get("sources", {}).items():
    if "jaffle_shop" in source_id and "customers" in source_id:
        print(f"Source: {source_id}")
        for col_name, col_meta in source.get("columns", {}).items():
            tags = col_meta.get("tags", [])
            if "pii" in tags:
                print(f"  Column {col_name} has PII tags: {tags}")

print("\n=== DEBUGGING STG_CUSTOMERS ===")
for node_id, node in manifest.get("nodes", {}).items():
    if "stg_customers" in node_id:
        print(f"Node: {node_id}")
        for col_name, col_meta in node.get("columns", {}).items():
            lineage = col_meta.get("lineage", [])
            tags = col_meta.get("tags", [])
            print(f"  Column {col_name}:")
            print(f"    Current tags: {tags}")
            print(f"    Lineage: {lineage}")
            for dep in lineage:
                dep_tags = get_column_tags(dep, col_name)
                print(f"    Dep {dep} tags: {dep_tags}")

print("\n=== MAIN ANALYSIS ===")
for node in manifest["nodes"].values():
    if "stg_customers" in node['unique_id']:
        print(f"$ node: {node['unique_id']}")
        for col_name, col_meta in node.get("columns", {}).items():
            print(f"$$ col_name: {col_name}")
            upstream_tags = set()
            for dep in col_meta.get("lineage", []):
                print(f"$$$ dep: {dep}")
                dep_tags = get_column_tags(dep, col_name)
                upstream_tags.update(dep_tags)
            print(f"$$$$ upstream_tags: {upstream_tags}")
            if "pii" in upstream_tags and "pii" not in col_meta.get("tags", []):
                print(f"⚠️ {node['unique_id']} column {col_name} missing propagated tag: pii")
