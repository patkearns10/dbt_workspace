import json
import csv

# Load your JSON data (replace with your file name)
with open('dbt_environments.json', 'r') as f:
    data = json.load(f)

# Output CSV path
output_file = 'dbt_environments_table.csv'

# Prepare the rows
rows = []

for env in data.get("data", []):
    connection = env.get("connection", {})
    
    row = {
        "project_id": env.get("project_id"),
        "environment_id": env.get("id"),
        "environment_name": env.get("name"),
        "connection_account": connection.get("account"),
        "connection_warehouse": connection.get("warehouse"),
        "connection_role": connection.get("role"),
        "connection_database": connection.get("database")
    }
    rows.append(row)

# Write to CSV
fieldnames = [
    "project_id",
    "environment_id",
    "environment_name",
    "connection_account",
    "connection_warehouse",
    "connection_role",
    "connection_database"
]

with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ CSV written to: {output_file}")
