import json
import os
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.patches as mpatches

# ---------- Helper Functions ----------

def get_top_folder(path):
    """Extract top-level folder from a dbt model path."""
    return path.split(os.sep)[0] if os.sep in path else path

def load_manifest(path="manifest.json"):
    with open(path, "r") as f:
        return json.load(f)

# ---------- Main Script ----------

# Load manifest
manifest = load_manifest("manifest.json")

# Prepare structures
edges = defaultdict(int)
folder_model_count = defaultdict(int)

# Parse model nodes
nodes = manifest.get("nodes", {})
for node_name, node_data in nodes.items():
    if not node_name.startswith("model."):
        continue

    target_path = node_data.get("original_file_path", "")
    target_folder = get_top_folder(target_path)
    folder_model_count[target_folder] += 1

    depends_on = node_data.get("depends_on", {}).get("nodes", [])
    for dep in depends_on:
        if not dep.startswith("model."):
            continue
        dep_node = manifest["nodes"].get(dep)
        if not dep_node:
            continue
        dep_path = dep_node.get("original_file_path", "")
        source_folder = get_top_folder(dep_path)
        if source_folder != target_folder:
            edges[(source_folder, target_folder)] += 1

# ---------- Build Graph ----------

G = nx.DiGraph()

# Add edges
for (src, dst), weight in edges.items():
    G.add_edge(src, dst, weight=weight)

# Add isolated folders (if any)
for folder in folder_model_count:
    if folder not in G.nodes:
        G.add_node(folder)

# Node sizes based on model count
def get_node_size(folder):
    base = 800
    scale = 250
    return base + folder_model_count[folder] * scale

node_sizes = [get_node_size(node) for node in G.nodes]

# ---------- Draw Graph ----------

plt.figure(figsize=(14, 10))

# Use Graphviz layout (dot = L to R)
pos = nx.nx_agraph.graphviz_layout(G, prog="dot")

# Apply spacing tweak
for k in pos:
    x, y = pos[k]
    pos[k] = (x * 1.2, y * 1.2)

# Draw nodes, edges, labels
nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color="skyblue", edgecolors="black")
nx.draw_networkx_labels(G, pos, font_weight="bold", font_size=10)
nx.draw_networkx_edges(G, pos, arrows=True, connectionstyle="arc3,rad=0.2")
nx.draw_networkx_edge_labels(G, pos, edge_labels={(u, v): d["weight"] for u, v, d in G.edges(data=True)}, font_size=8)

# ---------- Add Legend ----------

# Representative legend items
legend_counts = sorted(set(folder_model_count.values()))
if len(legend_counts) > 4:
    legend_counts = [min(legend_counts), 5, 15, max(legend_counts)]

legend_patches = [
    mpatches.Circle((0, 0), radius=0.25, facecolor="skyblue", edgecolor="black",
                    label=f"{count} model{'s' if count > 1 else ''}")
    for count in legend_counts
]
legend_sizes = [get_node_size(folder) for folder, count in folder_model_count.items() if count in legend_counts]

plt.legend(
    handles=legend_patches,
    labels=[f"{c} model{'s' if c > 1 else ''}" for c in legend_counts],
    loc="lower left",
    frameon=True,
    title="Model Count (Node Size)"
)

# ---------- Final Touch ----------

plt.title("DBT Folder Dependency Graph (Data Flow)", fontsize=14)
plt.axis("off")
plt.tight_layout()
plt.savefig("dbt_folder_dependency_graph.png")
plt.show()

# ---------- Save CSV ----------

df = pd.DataFrame([
    {"source_folder": src, "target_folder": dst, "num_dependencies": weight}
    for (src, dst), weight in edges.items()
])
df.to_csv("dbt_folder_dependencies.csv", index=False)
