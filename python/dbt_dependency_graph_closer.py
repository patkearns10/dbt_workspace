import json
import os
import pandas as pd
from collections import Counter, defaultdict
import networkx as nx
import plotly.graph_objects as go
import matplotlib.patches as mpatches
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
    pos = nx.spring_layout(graph, seed=42, k=1.2)  # smaller k to pull nodes closer
    
    # Fix integrations node position if it exists
    if "integrations" in pos:
        pos["integrations"] = (0, 0)
    
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

####

def draw_png_fixed_size_with_labels(graph, folder_model_counts, output_file="dbt_dependency_graph.png"):
    pos = nx.spring_layout(graph, seed=42, k=1.2)
    
    # Fix integrations position for png also
    if "integrations" in pos:
        pos["integrations"] = (0, 0)

    plt.figure(figsize=(14,10))
    ax = plt.gca()
    ax.set_aspect('equal')

    node_size_pt2 = 700  # same as used in draw_networkx_nodes
    # Convert node size from points^2 to radius in data coords:
    # Radius in points:
    node_radius_pt = (node_size_pt2 ** 0.5) / 2  
    # Get figure DPI and axis limits:
    fig = plt.gcf()
    dpi = fig.dpi

    xlim = ax.get_xlim()
    ylim = ax.get_ylim()

    x_range = abs(xlim[1] - xlim[0])
    y_range = abs(ylim[1] - ylim[0])

    # Calculate how many data units per point:
    data_per_point_x = x_range / (fig.get_size_inches()[0] * dpi)
    data_per_point_y = y_range / (fig.get_size_inches()[1] * dpi)
    # Approximate radius in data units (take average scale):
    node_radius = node_radius_pt * (data_per_point_x + data_per_point_y) / 2

    # Draw nodes fixed size
    nx.draw_networkx_nodes(
        graph, pos,
        node_size=node_size_pt2,
        node_color=[FOLDER_COLORS.get(node, "lightcoral") for node in graph.nodes()],
        edgecolors='black',
        ax=ax
    )

    # Draw edges with arrows that just touch node edges
    for (src, tgt) in graph.edges():
        x0, y0 = pos[src]
        x1, y1 = pos[tgt]

        dx = x1 - x0
        dy = y1 - y0
        dist = (dx**2 + dy**2)**0.5
        if dist == 0:
            continue

        # Offset start/end points by node radius along edge vector
        start_x = x0 + (dx / dist) * node_radius
        start_y = y0 + (dy / dist) * node_radius
        end_x = x1 - (dx / dist) * node_radius
        end_y = y1 - (dy / dist) * node_radius

        if graph.has_edge(tgt, src):
            edge_color = 'red'
            zorder = 3
        else:
            edge_color = 'gray'
            zorder = 2

        arrow = mpatches.FancyArrowPatch(
            (start_x, start_y), (end_x, end_y),
            arrowstyle='-|>', mutation_scale=15,
            linewidth=2, color=edge_color,
            alpha=0.7,
            zorder=zorder
        )
        ax.add_patch(arrow)

    # Draw labels
    nx.draw_networkx_labels(graph, pos, font_size=12, ax=ax)

    # Draw model count labels next to nodes with small offset
    for node, (x, y) in pos.items():
        count = folder_model_counts.get(node, 0)
        plt.text(x + 0.03, y, f"{count} models", fontsize=10, fontweight='bold', verticalalignment='center')

    plt.title("DBT Folder Dependency Graph (PNG with Model Count Labels & Bidirectional Edges)")
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    plt.close()
    print(f"PNG graph saved to {output_file}")

####

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
