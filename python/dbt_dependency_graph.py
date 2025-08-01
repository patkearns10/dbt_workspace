import json
import os
import pandas as pd
from collections import Counter, defaultdict
import networkx as nx
import plotly.graph_objects as go
import matplotlib.pyplot as plt

TOP_LEVEL_FOLDERS = [
    "integrations", "intermediate", "landing", "marts",
    "metrics", "projects", "staging"
]

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
    folder_model_counts = defaultdict(int)

    nodes = manifest.get("nodes", {})
    for node_name, node_data in nodes.items():
        if not node_name.startswith("model."):
            continue

        target_path = node_data.get("original_file_path", "")
        target_folder = get_top_folder(target_path)
        if target_folder:
            folder_model_counts[target_folder] += 1

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

    for folder in folder_model_counts:
        if folder not in graph:
            graph.add_node(folder)

    return graph, folder_edges, folder_model_counts

def draw_interactive_plotly(graph, folder_edges, folder_model_counts, output_file="dbt_dependency_graph.html"):
    pos = nx.spring_layout(graph, seed=42, k=3)  # layout

    node_x = []
    node_y = []
    node_text = []
    node_color = []
    node_size = []

    for node in graph.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        count = folder_model_counts.get(node, 1)
        node_text.append(f"{node}<br>{count} model{'s' if count > 1 else ''}")
        node_color.append(FOLDER_COLORS.get(node, "lightcoral"))
        node_size.append(25)  # Fixed size

    edge_x = []
    edge_y = []
    annotations = []

    for (src, tgt), weight in folder_edges.items():
        x0, y0 = pos[src]
        x1, y1 = pos[tgt]

        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

        annotations.append(
            dict(
                ax=x0,
                ay=y0,
                x=x1,
                y=y1,
                xref='x',
                yref='y',
                axref='x',
                ayref='y',
                showarrow=True,
                arrowhead=3,
                arrowsize=1.5,
                arrowwidth=1.5,
                arrowcolor='red' if graph.has_edge(tgt, src) else 'black',
                standoff=10,
                startstandoff=5,
                opacity=0.7,
                hovertext=f"Dependencies: {weight}",
                hoverlabel=dict(bgcolor='white'),
            )
        )

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers+text',
        text=[node for node in graph.nodes()],
        textposition="bottom center",
        hoverinfo='text',
        hovertext=node_text,
        marker=dict(
            color=node_color,
            size=node_size,
            line=dict(width=2, color='black')
        ),
        customdata=[folder_model_counts.get(node, 0) for node in graph.nodes()],
    )

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode='lines',
        line=dict(color='gray', width=1),
        hoverinfo='none'
    )

    layout = go.Layout(
        title="DBT Folder Dependency Graph (Plotly Interactive)",
        title_x=0.5,
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        annotations=annotations,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        dragmode='pan'
    )

    fig = go.Figure(data=[edge_trace, node_trace], layout=layout)
    fig.write_html(output_file)
    print(f"Interactive graph saved to {output_file}")

def draw_png_fixed_size_with_labels(graph, folder_model_counts, output_file="dbt_dependency_graph.png"):
    pos = nx.spring_layout(graph, seed=42, k=3)

    plt.figure(figsize=(14,10))

    nx.draw_networkx_nodes(
        graph, pos,
        node_size=700,
        node_color=[FOLDER_COLORS.get(node, "lightcoral") for node in graph.nodes()],
        edgecolors='black'
    )

    nx.draw_networkx_edges(graph, pos, arrows=True, arrowstyle='-|>', arrowsize=15)

    nx.draw_networkx_labels(graph, pos, font_size=12)

    for node, (x, y) in pos.items():
        count = folder_model_counts.get(node, 0)
        plt.text(x + 0.05, y + 0.05, f"{count} models", fontsize=10, fontweight='bold')

    plt.title("DBT Folder Dependency Graph (PNG with Model Count Labels)")
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    plt.close()
    print(f"PNG graph saved to {output_file}")

def save_csv(folder_edges, output_file="dbt_dependency_edges.csv"):
    data = [
        {"source_folder": source, "target_folder": target, "dependency_count": count}
        for (source, target), count in folder_edges.items()
    ]
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    print(f"CSV saved to {output_file}")

if __name__ == "__main__":
    graph, folder_edges, folder_model_counts = build_dependency_graph("manifest.json")
    draw_interactive_plotly(graph, folder_edges, folder_model_counts)
    draw_png_fixed_size_with_labels(graph, folder_model_counts)
    save_csv(folder_edges)
