import os
import shutil
import yaml

# Paths to folders (adjust as needed)
snapshots_folder = "snapshots"
models_folder = "models"
snapshots_yml_path = os.path.join(models_folder, "snapshots.yml")

# Ensure the models folder exists
os.makedirs(models_folder, exist_ok=True)

def move_snapshots_and_create_yml():
    snapshots_config = []

    for root, _, files in os.walk(snapshots_folder):
        for file in files:
            if file.endswith(".sql"):
                snapshot_path = os.path.join(root, file)

                # Read the snapshot file
                with open(snapshot_path, "r") as f:
                    snapshot_content = f.read()

                # Extract the snapshot name and config()
                snapshot_name = extract_snapshot_name(snapshot_content)
                snapshot_config = extract_snapshot_config(snapshot_content)

                # Generate new model content
                new_model_name = f"eph_{snapshot_name}"
                new_model_content = convert_to_ephemeral_model(snapshot_content)

                # Determine new file path in models folder
                relative_path = os.path.relpath(root, snapshots_folder)
                new_folder = os.path.join(models_folder, relative_path)
                os.makedirs(new_folder, exist_ok=True)
                new_model_path = os.path.join(new_folder, f"{new_model_name}.sql")

                # Write the new model file
                with open(new_model_path, "w") as f:
                    f.write(new_model_content)

                # Add snapshot metadata to config
                snapshots_config.append({
                    "name": snapshot_name,
                    "relation": f"ref('{new_model_name}')",
                    "config": snapshot_config
                })

                print(f"Moved and converted: {snapshot_path} -> {new_model_path}")

    # Write snapshots.yml
    formatted_config = format_snapshots_config(snapshots_config)
    with open(snapshots_yml_path, "w") as f:
        f.write(formatted_config)
    print(f"Generated {snapshots_yml_path}")

def extract_snapshot_name(content):
    """Extract the snapshot name from the content."""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("{% snapshot"):
            return line.split()[2]
    return None

def extract_snapshot_config(content):
    """Extract the config() dictionary from the content."""
    start = content.find("config(")
    end = content.find(")", start) + 1
    config_block = content[start:end]

    # Transform the config block into a Python dictionary
    config_str = config_block.replace("config(", "").strip("()")
    config_str = config_str.replace("true", "True").replace("false", "False").replace("null", "None")
    config_lines = config_str.split(",\n")
    config_dict = {}
    for line in config_lines:
        if "=" in line:
            key, value = line.split("=", 1)
            config_dict[key.strip()] = eval(value.strip())
    return config_dict

def format_snapshots_config(snapshots_config):
    """Format the snapshots config for YAML output with proper indentation."""
    formatted = "snapshots:\n"
    for snapshot in snapshots_config:
        formatted += f"  - name: {snapshot['name']}\n"
        formatted += f"    relation: {snapshot['relation']}\n"
        formatted += "    config:\n"
        for key, value in snapshot['config'].items():
            formatted += f"      {key}: {value}\n"
    return formatted

def convert_to_ephemeral_model(content):
    """Convert a snapshot file into an ephemeral model."""
    lines = content.splitlines()
    new_lines = []
    inside_config_block = False

    for line in lines:
        stripped_line = line.strip()

        if stripped_line.startswith("{% snapshot") or stripped_line.startswith("{% endsnapshot %}"):
            continue  # Skip the first and last Jinja control blocks

        if stripped_line.startswith("{{") and "config(" not in stripped_line:
            if "}}" in stripped_line:
                new_lines.append(line)  # Retain valid macros/functions
            continue

        if "config(" in stripped_line:
            inside_config_block = True
            new_lines.append("{{ config(materialized='ephemeral') }}")  # Add only ephemeral config
            continue

        if inside_config_block:
            if stripped_line.endswith("}}"):
                inside_config_block = False  # End of config block
            continue  # Skip lines inside the original config block

        new_lines.append(line)

    return "\n".join(new_lines)

# Run the script
move_snapshots_and_create_yml()
