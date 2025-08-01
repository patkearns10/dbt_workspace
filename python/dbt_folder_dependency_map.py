import json
import os
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter

# Define folders you're interested in
TOP_LEVEL_FOLDERS = [
    "integrations", "intermediate", "landing", "marts",
    "metrics", "projects", "staging"
]

# Folder color palette
FOLDER_COLORS = {
    "landing": "lightgray",
    "staging": "lightblue",
    "intermediate": "lightgreen",
    "marts": "orange",
    "metrics": "pink",
    "projects": "violet",
    "integrations": "khaki"
}

def get_top_folder(file_path):
    parts = file_path.split(os.sep)
    if "models" in parts:
        idx = parts.index("models")
        if len(parts) > idx + 1:
            folder = parts[idx + 1]
            if folder in TOP_LEVEL_FOLDERS:
                return folder
    return None

def build_dependency_graph(manifest_path="manifest.json"):
    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    graph = nx.DiGraph()
    folder_edges = Counter()

    nodes = manifest.get("nodes", {})
    for node_name, node_data in nodes.items():
        if not node_name.startswith("model."):
            continue

        target_path = node_data.get("original_file_path", "")
        target_folder = get_top_folder(target_path)
        depends_on = node_data.get("depends_on", {}).get("nodes", [])

        for dep in depends_on:
            dep_data = nodes.get(dep)
            if dep_data:
                source_path = dep_data.get("original_file_path", "")
                source_folder = get_top_folder(source_path)

                if source_folder and target_folder and source_folder != target_folder:
                    folder_edges[(source_folder, target_folder)] += 1

    for (source, target), weight in folder_edges.items():
        graph.add_edge(source, target, weight=weight)

    return graph, folder_edges

def draw_and_save_graph(graph, folder_edges, output_file="dbt_dependency_graph.png"):
    pos = nx.kamada_kawai_layout(graph)  # improved layout

    node_colors = [FOLDER_COLORS.get(node, "lightcoral") for node in graph.nodes()]
    edge_labels = {(u, v): f"{d['weight']}" for u, v, d in graph.edges(data=True)}

    plt.figure(figsize=(12, 8))
    nx.draw_networkx_nodes(graph, pos, node_size=3000, node_color=node_colors)
    nx.draw_networkx_edges(graph, pos, arrows=True, arrowstyle="->", width=1)
    nx.draw_networkx_labels(graph, pos, font_size=10, font_weight="bold")
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, font_size=9)

    plt.title("DBT Folder Dependency Graph (Data Flow)", fontsize=14)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    print(f"Graph saved to {output_file}")

def save_csv(folder_edges, output_file="dbt_dependency_edges.csv"):
    data = [
        {"source_folder": source, "target_folder": target, "dependency_count": count}
        for (source, target), count in folder_edges.items()
    ]
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    print(f"CSV saved to {output_file}")

if __name__ == "__main__":
    graph, folder_edges = build_dependency_graph("manifest.json")
    draw_and_save_graph(graph, folder_edges)
    save_csv(folder_edges)